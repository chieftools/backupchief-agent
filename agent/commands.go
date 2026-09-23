package agent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/chieftools/backupchief-agent/restic"
	"github.com/chieftools/backupchief-agent/updater"
)

func (daemon *daemon) processCommands(ctx context.Context) error {
	daemon.mu.Lock()
	paused := daemon.state.AuthenticationPaused
	daemon.mu.Unlock()
	if paused {
		return nil
	}

	response, err := daemon.client.PollCommands(ctx, daemon.bootstrap.Generation)
	if err != nil {
		return daemon.handleRequestError(err)
	}
	for _, command := range response.Commands {
		if err := daemon.receiveCommand(command); err != nil {
			return err
		}
	}
	return daemon.clearAuthenticationPause()
}

func (daemon *daemon) dispatchCommands(ctx context.Context) error {
	if err := daemon.consumeAgentUpdateResult(); err != nil {
		return err
	}
	if err := daemon.reconcileInterrupted(ctx); err != nil {
		return daemon.handleRequestError(err)
	}
	daemon.mu.Lock()
	paused := daemon.state.AuthenticationPaused
	daemon.mu.Unlock()
	if paused {
		return nil
	}
	err := daemon.advanceCommands(ctx)
	daemon.resumeReplications(ctx)
	return daemon.handleRequestError(err)
}

func (daemon *daemon) reportJournal(ctx context.Context) error {
	return daemon.handleRequestError(daemon.flushJournal(ctx))
}

func (daemon *daemon) receiveCommand(command AgentCommand) error {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()

	if existing := daemon.journal.Commands[command.ID]; existing != nil {
		if !sameCommand(existing.Command, command) {
			return fmt.Errorf("redelivered command %s changed content", command.ID)
		}
		return nil
	}

	runID := command.Payload.RunID
	if command.Kind == "run_backup" || command.Kind == "run_maintenance" || command.Kind == "inspect_source" || command.Kind == "sync_replica" || command.Kind == "update_agent" {
		var err error
		runID, err = newULID(daemon.now())
		if err != nil {
			return err
		}
	}

	runKind := command.Payload.Maintenance
	if command.Kind == "run_backup" {
		runKind = "backup"
	} else if command.Kind == "inspect_source" {
		runKind = "inspection"
	} else if command.Kind == "sync_replica" {
		runKind = "replica_sync"
	} else if command.Kind == "update_agent" {
		runKind = "agent_update"
	}

	daemon.journal.Commands[command.ID] = &JournalCommand{
		Command:         command,
		RunID:           runID,
		RunKind:         runKind,
		ConfigRevision:  command.Payload.RequiredConfigRevision,
		Trigger:         "manual",
		ReceivedAt:      protocolTimestamp(daemon.now()),
		State:           "received",
		Events:          []AgentEvent{},
		MaintenancePlan: command.Payload.MaintenancePlan,
	}
	if err := daemon.store.SaveCommandJournal(daemon.journal); err != nil {
		delete(daemon.journal.Commands, command.ID)
		return err
	}

	if command.Kind == "update_agent" {
		previous := daemon.state.AgentUpdate
		daemon.state.AgentUpdate = &AgentUpdateRuntime{
			CommandID:          command.ID,
			RunID:              runID,
			TargetVersion:      command.Payload.TargetVersion,
			StartedAt:          protocolTimestamp(daemon.now()),
			LastScheduleMinute: protocolTimestamp(daemon.lastScheduleMinute),
		}
		if err := daemon.store.SaveRuntimeState(daemon.state); err != nil {
			daemon.state.AgentUpdate = previous
			delete(daemon.journal.Commands, command.ID)
			_ = daemon.store.SaveCommandJournal(daemon.journal)
			return err
		}
	}

	notifyLoop(daemon.dispatchWake)
	return nil
}

func (daemon *daemon) advanceCommands(ctx context.Context) error {
	daemon.mu.Lock()
	ids := make([]string, 0, len(daemon.journal.Commands))
	for id := range daemon.journal.Commands {
		ids = append(ids, id)
	}
	daemon.mu.Unlock()
	sort.Strings(ids)

	var dispatchErrors []error
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := daemon.advanceCommand(ctx, id); err != nil {
			dispatchErrors = append(dispatchErrors, err)
		}
	}
	return errors.Join(dispatchErrors...)
}

func (daemon *daemon) advanceCommand(ctx context.Context, id string) (dispatchErr error) {
	daemon.mu.Lock()
	journaled := daemon.journal.Commands[id]
	if journaled == nil {
		daemon.mu.Unlock()
		return nil
	}
	acknowledged := journaled.Acknowledged
	command := journaled.Command
	runID := journaled.RunID
	state := journaled.State
	receivedAt := journaled.ReceivedAt
	daemon.mu.Unlock()
	defer func() {
		if dispatchErr != nil {
			var apiError *APIError
			if errors.As(dispatchErr, &apiError) {
				log.Printf("backupchief: command dispatch failed for %s (%s): API status=%d code=%s", id, command.Kind, apiError.Status, apiError.Code)
			} else {
				log.Printf("backupchief: command dispatch failed for %s (%s): %v", id, command.Kind, dispatchErr)
			}
			dispatchErr = fmt.Errorf("command %s (%s): %w", id, command.Kind, dispatchErr)
		}
	}()

	if !acknowledged {
		discarded, err := daemon.discardExpiredUnacknowledgedCommand(id)
		if err != nil {
			return err
		}
		if discarded {
			return nil
		}

		err = daemon.client.AcknowledgeCommand(ctx, command.ID, CommandAcknowledgement{
			Generation: command.Generation,
			RunID:      runID,
			ReceivedAt: receivedAt,
		})
		if err != nil {
			return err
		}
		daemon.mu.Lock()
		if current := daemon.journal.Commands[id]; current != nil {
			current.Acknowledged = true
			err = daemon.store.SaveCommandJournal(daemon.journal)
		}
		daemon.mu.Unlock()
		if err != nil {
			return err
		}
	}

	if command.Kind == "cancel_run" {
		if err := daemon.cancelRun(runID); err != nil {
			return err
		}
		daemon.mu.Lock()
		delete(daemon.journal.Commands, id)
		err := daemon.store.SaveCommandJournal(daemon.journal)
		daemon.mu.Unlock()
		if err != nil {
			return err
		}
		return nil
	}
	if command.Kind == "update_agent" {
		if state == "received" {
			if err := daemon.startAgentUpdate(id); err != nil {
				return err
			}
		}
		return nil
	}
	daemon.mu.Lock()
	draining := daemon.state.AgentUpdate != nil
	daemon.mu.Unlock()
	if draining {
		return nil
	}
	if command.Kind == "inspect_source" && state == "received" {
		result := inspectSource(ctx, daemon.bootstrap.Generation, runID, daemon.store.stateDirectory(), command.Payload)
		if result.Status == "failed" {
			logInspectionFailure(runID, result.Failure)
		}
		daemon.mu.Lock()
		if current := daemon.journal.Commands[id]; current != nil {
			current.State = "finished"
			current.Result = &result
			if err := daemon.store.SaveCommandJournal(daemon.journal); err != nil {
				daemon.mu.Unlock()
				return err
			}
		}
		daemon.mu.Unlock()
		return nil
	}
	if (!contains([]string{"run_backup", "run_maintenance", "sync_replica"}, command.Kind) && !strings.HasPrefix(command.Kind, "scheduled_")) || state != "received" {
		return nil
	}
	if journaled.WaitForBackup {
		return nil
	}

	expiresAt, _ := time.Parse("2006-01-02T15:04:05.000000Z", command.ExpiresAt)
	if (command.Kind == "run_backup" || command.Kind == "run_maintenance" || command.Kind == "sync_replica") && !daemon.now().Before(expiresAt) {
		daemon.mu.Lock()
		configUnavailable := daemon.metadata.Revision < command.Payload.RequiredConfigRevision
		daemon.mu.Unlock()
		var finishErr error
		if configUnavailable {
			finishErr = daemon.finishWithoutExecution(id, "skipped", "config_unavailable", "The required configuration was unavailable before expiry.")
		} else {
			finishErr = daemon.finishWithoutExecution(id, "skipped", "expired", "The command expired before execution.")
		}
		if finishErr != nil {
			return finishErr
		}
		return nil
	}

	job, ready, err := daemon.jobForExecution(ctx, journaled)
	if err != nil {
		if journaled.Trigger == "scheduled" && errors.Is(err, errInvalidJobSnapshot) {
			return daemon.finishWithoutExecution(id, "skipped", "config_unavailable", "The saved job configuration is invalid.")
		}
		return err
	}
	if !ready {
		return nil
	}
	if job == nil || !job.Enabled {
		if err := daemon.finishWithoutExecution(id, "skipped", "config_unavailable", "The required job configuration is unavailable."); err != nil {
			return err
		}
		return nil
	}
	if command.Kind == "sync_replica" {
		if err := daemon.startReplicaSync(ctx, id, *job); err != nil {
			return err
		}
		return nil
	}
	if err := daemon.startOperation(ctx, id, *job); err != nil {
		return err
	}
	return nil
}

func (daemon *daemon) discardExpiredUnacknowledgedCommand(commandID string) (bool, error) {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()

	journaled := daemon.journal.Commands[commandID]
	if journaled == nil || journaled.Acknowledged || journaled.Trigger != "manual" {
		return false, nil
	}
	expiresAt, err := time.Parse("2006-01-02T15:04:05.000000Z", journaled.Command.ExpiresAt)
	if err != nil || daemon.now().Before(expiresAt) {
		return false, nil
	}

	delete(daemon.journal.Commands, commandID)
	if err := daemon.store.SaveCommandJournal(daemon.journal); err != nil {
		daemon.journal.Commands[commandID] = journaled
		return false, err
	}

	if daemon.state.AgentUpdate != nil && daemon.state.AgentUpdate.CommandID == commandID {
		daemon.state.AgentUpdate = nil
		if err := daemon.store.SaveRuntimeState(daemon.state); err != nil {
			return false, err
		}
	}

	log.Printf("backupchief: discarded expired unacknowledged command %s", commandID)
	return true, nil
}

func (daemon *daemon) configuredJob(ctx context.Context, jobID string, requiredRevision uint64) (*Job, bool, error) {
	daemon.mu.Lock()
	revision := daemon.metadata.Revision
	daemon.mu.Unlock()
	if revision < requiredRevision {
		if err := daemon.refreshConfig(ctx); err != nil {
			return nil, false, err
		}
	}
	body, metadata, err := daemon.store.LoadConfig(daemon.bootstrap)
	if err != nil {
		return nil, false, err
	}
	if metadata.Revision < requiredRevision {
		return nil, false, nil
	}
	config, _, err := DecodeConfig(body, daemon.bootstrap.Generation)
	if err != nil {
		return nil, false, err
	}
	for index := range config.Jobs {
		if config.Jobs[index].ID == jobID {
			return &config.Jobs[index], true, nil
		}
	}
	return nil, true, nil
}

func (daemon *daemon) jobForExecution(ctx context.Context, command *JournalCommand) (*Job, bool, error) {
	if command.JobSnapshot != "" {
		job, err := decodeJobSnapshot(daemon.bootstrap, command.RunID, command.ConfigRevision, command.JobSnapshot)
		if err != nil {
			return nil, false, err
		}
		return &job, true, nil
	}
	return daemon.configuredJob(ctx, command.Command.Payload.JobID, command.Command.Payload.RequiredConfigRevision)
}

func (daemon *daemon) startOperation(ctx context.Context, commandID string, job Job) error {
	if !daemon.store.canAcceptWork(criticalRunRecordEstimate) {
		return nil
	}

	daemon.mu.Lock()
	if daemon.state.AgentUpdate != nil {
		daemon.mu.Unlock()
		return nil
	}
	journaled := daemon.journal.Commands[commandID]
	if journaled == nil || journaled.State != "received" {
		daemon.mu.Unlock()
		return nil
	}
	if journaled.RunKind == "" {
		journaled.RunKind = "backup"
	}

	if daemon.active == nil {
		daemon.active = make(map[string]context.CancelFunc)
	}
	if daemon.activeRunKinds == nil {
		daemon.activeRunKinds = make(map[string]string)
	}
	if daemon.repositories == nil {
		daemon.repositories = make(map[string]bool)
	}

	backups, maintenance := daemon.activeCountsLocked()
	capacityReached := journaled.RunKind == "backup" && backups >= 2 || journaled.RunKind != "backup" && maintenance >= 1
	repositoryIDs := []string{job.Repository.ID}
	if journaled.RunKind != "backup" {
		for _, replica := range job.Replicas {
			repositoryIDs = append(repositoryIDs, replica.ID)
		}
		if daemon.replicationActive[job.ID] || daemon.hasPendingReplicationLocked(job.ID) {
			daemon.mu.Unlock()
			return nil
		}
	}

	repositoryBusy := false
	for _, repositoryID := range repositoryIDs {
		repositoryBusy = repositoryBusy || daemon.repositories[repositoryID]
	}
	if capacityReached || repositoryBusy {
		daemon.mu.Unlock()
		return nil
	}

	var plan *MaintenancePlan
	if journaled.RunKind != "backup" {
		job, plan = daemon.prepareMaintenanceLocked(job, journaled)
		if err := daemon.store.SaveRuntimeState(daemon.state); err != nil {
			daemon.mu.Unlock()
			return err
		}
	}

	runContext, cancel := context.WithCancel(ctx)
	initialSequence := journaled.Sequence
	initialEventCount := len(journaled.Events)
	if journaled.JobSnapshot == "" {
		journaled.ConfigRevision = journaled.Command.Payload.RequiredConfigRevision
		snapshot, err := encodeJobSnapshot(daemon.bootstrap, journaled.RunID, journaled.ConfigRevision, job)
		if err != nil {
			cancel()
			daemon.mu.Unlock()
			return err
		}
		journaled.JobSnapshot = snapshot
	}

	journaled.State = "running"
	journaled.Sequence++
	eventID, err := newULID(daemon.now())
	if err != nil {
		cancel()
		daemon.mu.Unlock()
		return err
	}
	journaled.Events = append(journaled.Events, daemon.eventEnvelope(
		journaled, eventID, journaled.Sequence, protocolTimestamp(daemon.now()), "run_started", map[string]any{},
	))

	if absInt64(daemon.state.ClockOffsetSeconds) > 300 {
		journaled.Sequence++
		if skewEventID, skewErr := newULID(daemon.now()); skewErr == nil {
			journaled.Events = append(journaled.Events, daemon.eventEnvelope(
				journaled, skewEventID, journaled.Sequence, protocolTimestamp(daemon.now()), "clock_skew",
				map[string]any{"offset_seconds": daemon.state.ClockOffsetSeconds},
			))
		}
	}
	if err := daemon.store.SaveCommandJournal(daemon.journal); err != nil {
		cancel()
		journaled.State = "received"
		journaled.Sequence = initialSequence
		journaled.Events = journaled.Events[:initialEventCount]
		daemon.mu.Unlock()
		return err
	}

	daemon.active[journaled.RunID] = cancel
	daemon.activeRunKinds[journaled.RunID] = journaled.RunKind
	for _, repositoryID := range repositoryIDs {
		daemon.repositories[repositoryID] = true
	}
	daemon.activeWG.Add(1)
	runID := journaled.RunID
	runKind := journaled.RunKind
	daemon.mu.Unlock()

	if runKind == "backup" {
		go daemon.runBackup(runContext, commandID, runID, job)
	} else {
		go daemon.runMaintenance(runContext, commandID, runID, job, plan)
	}

	return nil
}

func (daemon *daemon) startBackup(ctx context.Context, commandID string, job Job) error {
	return daemon.startOperation(ctx, commandID, job)
}

func (daemon *daemon) runBackup(ctx context.Context, commandID, runID string, job Job) {
	defer daemon.activeWG.Done()
	result, log, truncated, dropped := executeBackup(
		ctx, daemon.executor, daemon.store.stateDirectory(), daemon.bootstrap.ServerID, daemon.bootstrap.Generation,
		daemon.journalCommand(commandID), job, daemon.now, daemon.databaseBackupProgress(commandID),
	)
	if ctx.Err() == nil {
		result.RepositoryBytes = measureRepositoryBytes(ctx, daemon.executor, job)
	}
	daemon.finishOperation(ctx, commandID, runID, job, result, log, truncated, dropped)
}

func (daemon *daemon) databaseBackupProgress(commandID string) DatabaseBackupProgress {
	return func(artifact BackupArtifact, completed, total int) {
		if daemon.client == nil || !protocolRevisionSupports(daemon.client.selectedProtocolRevision(), "1.6.0") {
			return
		}

		daemon.mu.Lock()
		journaled := daemon.journal.Commands[commandID]
		if journaled == nil || journaled.State != "running" {
			daemon.mu.Unlock()
			return
		}

		eventID, err := newULID(daemon.now())
		if err != nil {
			daemon.mu.Unlock()
			return
		}

		journaled.Sequence++
		journaled.Events = append(journaled.Events, daemon.eventEnvelope(
			journaled,
			eventID,
			journaled.Sequence,
			protocolTimestamp(daemon.now()),
			"run_progress",
			map[string]any{
				"database_snapshot":   artifact,
				"databases_completed": completed,
				"databases_total":     total,
			},
		))
		if err := daemon.store.SaveCommandJournal(daemon.journal); err != nil {
			journaled.Sequence--
			journaled.Events = journaled.Events[:len(journaled.Events)-1]
			if errors.Is(err, ErrSpoolCapacity) {
				for _, cancel := range daemon.active {
					cancel()
				}
			}
			daemon.mu.Unlock()
			return
		}
		daemon.mu.Unlock()

		notifyLoop(daemon.reportWake)
	}
}

func (daemon *daemon) runMaintenance(ctx context.Context, commandID, runID string, job Job, plan *MaintenancePlan) {
	defer daemon.activeWG.Done()
	persistPlan := func(updated MaintenancePlan) error {
		daemon.mu.Lock()
		defer daemon.mu.Unlock()
		journaled := daemon.journal.Commands[commandID]
		if journaled == nil {
			return nil
		}
		journaled.MaintenancePlan = &updated
		return daemon.store.SaveCommandJournal(daemon.journal)
	}
	result, log, truncated, dropped := executeMaintenanceRepositories(
		ctx, daemon.executor, daemon.bootstrap.Generation, daemon.journalCommand(commandID), job, plan,
		persistPlan, daemon.now, daemon.maintenanceRetries,
	)
	daemon.finishOperation(ctx, commandID, runID, job, result, log, truncated, dropped)
}

func (daemon *daemon) finishOperation(ctx context.Context, commandID, runID string, job Job, result CommandResult, log []byte, truncated bool, dropped uint64) {
	if daemon.client != nil && !protocolRevisionSupports(daemon.client.selectedProtocolRevision(), "1.2.0") {
		result.SnapshotEvidence = nil
		result.SnapshotEvidenceIDs = nil
	}
	if daemon.client != nil && !protocolRevisionSupports(daemon.client.selectedProtocolRevision(), "1.4.0") && result.Statistics != nil {
		delete(*result.Statistics, "prune_status")
	}

	logID, idErr := newULID(daemon.now())
	if idErr == nil {
		idErr = daemon.store.WriteRunLog(logID, log)
	}

	daemon.mu.Lock()
	delete(daemon.active, runID)
	delete(daemon.activeRunKinds, runID)
	delete(daemon.repositories, job.Repository.ID)
	for _, replica := range job.Replicas {
		delete(daemon.repositories, replica.ID)
	}

	journaled := daemon.journal.Commands[commandID]
	if journaled == nil {
		daemon.mu.Unlock()
		return
	}
	journaled.State = "finished"
	journaled.Result = &result
	if result.RunKind == "backup" && (result.Status == "complete" || result.Status == "partial") && len(result.SnapshotIDs) > 0 && len(job.Replicas) > 0 {
		journaled.ReplicationPending = make([]string, 0, len(job.Replicas))
		for _, replica := range job.Replicas {
			journaled.ReplicationPending = append(journaled.ReplicationPending, replica.Key)
		}
	}

	daemon.appendSnapshotEvidenceEvents(journaled, result)
	journaled.Sequence++
	if eventID, err := newULID(daemon.now()); err == nil {
		journaled.Events = append(journaled.Events, daemon.eventEnvelope(
			journaled, eventID, journaled.Sequence, result.FinishedAt, "run_finished", terminalEventPayload(result),
		))
	}
	journaled.ResultReported = journaled.Trigger == "scheduled"
	if idErr == nil {
		journaled.LogID = logID
		journaled.LogBytes = len(log)
		journaled.LogSHA256 = fmt.Sprintf("%x", sha256.Sum256(log))
		journaled.LogTruncated = truncated
		journaled.LogDroppedBytes = dropped
	}

	if idErr != nil {
		reason := "log_unavailable"
		if errors.Is(idErr, ErrSpoolCapacity) {
			reason = "spool_pressure"
		}
		journaled.Sequence++
		if gapEventID, gapErr := newULID(daemon.now()); gapErr == nil {
			journaled.Events = append(journaled.Events, daemon.eventEnvelope(
				journaled, gapEventID, journaled.Sequence, result.FinishedAt, "reporting_gap",
				map[string]any{"reason": reason, "dropped_event_count": uint64(0), "dropped_log_bytes": uint64(len(log)) + dropped},
			))
		}
		daemon.markSpoolGapLocked()
		_ = daemon.store.SaveRuntimeState(daemon.state)
	}

	daemon.recordOutcomeLocked(job, result)
	if err := daemon.store.SaveCommandJournal(daemon.journal); errors.Is(err, ErrSpoolCapacity) {
		for _, cancel := range daemon.active {
			cancel()
		}
		_ = daemon.store.SaveCommandJournal(daemon.journal)
	}
	releaseMaintenance := journaled.Trigger == "scheduled" && result.RunKind == "backup" && result.Status == "complete"
	releaseCatchUp := result.RunKind != "backup"
	jobID := journaled.Command.Payload.JobID
	hasReplication := len(journaled.ReplicationPending) > 0
	daemon.mu.Unlock()

	if hasReplication {
		daemon.startReplication(ctx, job)
	}
	if releaseMaintenance && !hasReplication {
		_ = daemon.startNextDeferredMaintenance(context.Background(), jobID, false)
	}
	if releaseCatchUp {
		_ = daemon.startCatchUpBackup(context.Background(), jobID)
	}
}

func (daemon *daemon) hasPendingReplicationLocked(jobID string) bool {
	for _, command := range daemon.journal.Commands {
		if command.Result != nil && command.Result.JobID == jobID && len(command.ReplicationPending) > 0 {
			return true
		}
	}
	return false
}

func (daemon *daemon) appendSnapshotEvidenceEvents(command *JournalCommand, result CommandResult) {
	if len(result.RepositoryResults) > 0 {
		for _, repositoryResult := range result.RepositoryResults {
			daemon.appendRepositorySnapshotEvidenceEvents(command, repositoryResult)
		}
		return
	}
	if result.SnapshotEvidence == nil || len(result.SnapshotEvidenceIDs) == 0 {
		return
	}
	for offset := 0; offset < len(result.SnapshotEvidenceIDs); offset += snapshotEvidenceChunkSize {
		end := min(offset+snapshotEvidenceChunkSize, len(result.SnapshotEvidenceIDs))
		command.Sequence++
		eventID, err := newULID(daemon.now())
		if err != nil {
			return
		}
		command.Events = append(command.Events, daemon.eventEnvelope(
			command, eventID, command.Sequence, result.SnapshotEvidence.ObservedAt, "snapshot_inventory_chunk",
			map[string]any{
				"scope":        result.SnapshotEvidence.Scope,
				"chunk_index":  offset / snapshotEvidenceChunkSize,
				"snapshot_ids": append([]string(nil), result.SnapshotEvidenceIDs[offset:end]...),
			},
		))
	}
}

func (daemon *daemon) appendRepositorySnapshotEvidenceEvents(command *JournalCommand, result RepositoryResult) {
	if result.SnapshotEvidence == nil || len(result.SnapshotEvidenceIDs) == 0 {
		return
	}
	for offset := 0; offset < len(result.SnapshotEvidenceIDs); offset += snapshotEvidenceChunkSize {
		end := min(offset+snapshotEvidenceChunkSize, len(result.SnapshotEvidenceIDs))
		command.Sequence++
		eventID, err := newULID(daemon.now())
		if err != nil {
			return
		}
		command.Events = append(command.Events, daemon.eventEnvelope(
			command, eventID, command.Sequence, result.SnapshotEvidence.ObservedAt, "snapshot_inventory_chunk",
			map[string]any{
				"repository_key": result.RepositoryKey,
				"scope":          result.SnapshotEvidence.Scope, "chunk_index": offset / snapshotEvidenceChunkSize,
				"snapshot_ids": append([]string(nil), result.SnapshotEvidenceIDs[offset:end]...),
			},
		))
	}
}

func (daemon *daemon) journalCommand(commandID string) *JournalCommand {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	return daemon.journal.Commands[commandID]
}

func (daemon *daemon) finishWithoutExecution(commandID, status, resultCode, summary string) error {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()

	journaled := daemon.journal.Commands[commandID]
	if journaled == nil {
		return nil
	}

	now := protocolTimestamp(daemon.now())
	journaled.State = "finished"
	if journaled.Command.Kind == "sync_replica" {
		if !contains([]string{"copy_failed", "cancelled", "timed_out", "execution_failed"}, resultCode) {
			resultCode = "execution_failed"
		}
		status = "failed"
		if resultCode == "cancelled" {
			status = "cancelled"
		}
		journaled.Result = &CommandResult{
			Generation:    daemon.bootstrap.Generation,
			RunID:         journaled.RunID,
			JobID:         journaled.Command.Payload.JobID,
			RepositoryKey: journaled.Command.Payload.RepositoryKey,
			Status:        status,
			ResultCode:    resultCode,
			StartedAt:     now,
			FinishedAt:    now,
			SnapshotIDs:   []string{},
			Summary:       summary,
		}
		journaled.ResultReported = false
		return daemon.store.SaveCommandJournal(daemon.journal)
	}

	journaled.Result = &CommandResult{
		Generation:  daemon.bootstrap.Generation,
		RunID:       journaled.RunID,
		JobID:       journaled.Command.Payload.JobID,
		RunKind:     journaled.RunKind,
		Status:      status,
		ResultCode:  resultCode,
		StartedAt:   now,
		FinishedAt:  now,
		SnapshotIDs: []string{},
		Summary:     summary,
	}

	journaled.Sequence++
	if eventID, err := newULID(daemon.now()); err == nil {
		journaled.Events = append(journaled.Events, daemon.eventEnvelope(
			journaled, eventID, journaled.Sequence, now, "run_finished", terminalEventPayload(*journaled.Result),
		))
	}

	journaled.ResultReported = journaled.Trigger == "scheduled"
	return daemon.store.SaveCommandJournal(daemon.journal)
}

func (daemon *daemon) cancelRun(runID string) error {
	daemon.mu.Lock()
	cancel := daemon.active[runID]
	commandID := ""
	if cancel == nil {
		for id, command := range daemon.journal.Commands {
			if (command.Command.Kind == "run_backup" || command.Command.Kind == "run_maintenance") && command.RunID == runID && command.State == "received" {
				commandID = id
				break
			}
		}
	}
	daemon.mu.Unlock()
	if cancel != nil {
		cancel()
		return nil
	}
	if commandID != "" {
		return daemon.finishWithoutExecution(commandID, "cancelled", "cancelled", "The operation was cancelled before execution.")
	}
	return nil
}

func (daemon *daemon) flushJournal(ctx context.Context) error {
	daemon.mu.Lock()
	if err := daemon.store.SaveCommandJournal(daemon.journal); err != nil {
		daemon.mu.Unlock()
		return err
	}
	ids := make([]string, 0, len(daemon.journal.Commands))
	for id := range daemon.journal.Commands {
		if daemon.journal.Commands[id].Command.Generation == daemon.bootstrap.Generation {
			ids = append(ids, id)
		}
	}
	daemon.mu.Unlock()
	sort.Strings(ids)
	var firstError error
	for _, id := range ids {
		if err := daemon.flushCommand(ctx, id); err != nil {
			if firstError == nil {
				firstError = err
			}
		}
	}
	return firstError
}

func (daemon *daemon) flushCommand(ctx context.Context, commandID string) error {
	daemon.mu.Lock()
	journaled := daemon.journal.Commands[commandID]
	if journaled == nil || !journaled.Acknowledged {
		daemon.mu.Unlock()
		return nil
	}
	events := append([]AgentEvent(nil), journaled.Events...)
	runID := journaled.RunID
	logID := journaled.LogID
	nextChunk := journaled.NextLogChunk
	logCompleted := journaled.LogCompleted
	result := journaled.Result
	updateResult := journaled.UpdateResult
	resultReported := journaled.ResultReported
	command := journaled.Command
	daemon.mu.Unlock()

	if len(events) > 0 {
		batch := events[:min(len(events), maximumEventBatch)]
		response, err := daemon.client.SubmitEvents(ctx, EventRequest{Generation: daemon.bootstrap.Generation, Events: batch})
		if err != nil {
			return err
		}
		retained := make([]AgentEvent, 0, len(events))
		var rejection error
		discarded := false
		for index, item := range response.Results {
			if item.Status != "rejected" {
				continue
			}
			if batch[index].Kind == "run_finished" || batch[index].Kind == "snapshot_inventory_chunk" || batch[index].Kind == "replication_finished" {
				retained = append(retained, batch[index])
			} else {
				discarded = true
			}
			rejection = fmt.Errorf("event %s was rejected: %s", item.ID, item.Code)
		}
		daemon.mu.Lock()
		if current := daemon.journal.Commands[commandID]; current != nil {
			current.Events = append(retained, current.Events[len(batch):]...)
			if discarded {
				daemon.markSpoolGapLocked()
			}
			if err := daemon.store.SaveCommandJournal(daemon.journal); err != nil {
				daemon.mu.Unlock()
				return err
			}
		}
		if discarded {
			if err := daemon.store.SaveRuntimeState(daemon.state); err != nil {
				daemon.mu.Unlock()
				return err
			}
		}
		daemon.mu.Unlock()
		if rejection != nil {
			return rejection
		}
		if len(events) > len(batch) {
			return nil
		}
	}

	if logID != "" && !logCompleted {
		log, err := daemon.store.ReadChecksummedRunLog(logID, journaled.LogSHA256)
		if err != nil {
			return err
		}
		for offset := nextChunk * maximumLogChunk; offset < len(log); offset += maximumLogChunk {
			end := min(offset+maximumLogChunk, len(log))
			if err := daemon.client.UploadLogChunk(ctx, runID, logID, offset/maximumLogChunk, offset, log[offset:end]); err != nil {
				return err
			}
			daemon.mu.Lock()
			if current := daemon.journal.Commands[commandID]; current != nil {
				current.NextLogChunk = offset/maximumLogChunk + 1
				err = daemon.store.SaveCommandJournal(daemon.journal)
			}
			daemon.mu.Unlock()
			if err != nil {
				return err
			}
		}
		daemon.mu.Lock()
		current := daemon.journal.Commands[commandID]
		completion := LogCompletion{}
		if current != nil {
			completion = LogCompletion{
				Generation:   daemon.bootstrap.Generation,
				TotalBytes:   current.LogBytes,
				SHA256:       current.LogSHA256,
				ChunkCount:   current.NextLogChunk,
				Truncated:    current.LogTruncated,
				DroppedBytes: current.LogDroppedBytes,
			}
		}
		daemon.mu.Unlock()
		if current != nil {
			if err := daemon.client.CompleteLog(ctx, runID, logID, completion); err != nil {
				return err
			}
			daemon.mu.Lock()
			if current = daemon.journal.Commands[commandID]; current != nil {
				current.LogCompleted = true
				err = daemon.store.SaveCommandJournal(daemon.journal)
			}
			daemon.mu.Unlock()
			if err != nil {
				return err
			}
		}
	}

	if updateResult != nil && !resultReported {
		if err := daemon.client.SubmitAgentUpdateResult(ctx, command.ID, *updateResult); err != nil {
			return err
		}
		daemon.mu.Lock()
		if current := daemon.journal.Commands[commandID]; current != nil {
			current.ResultReported = true
			if err := daemon.store.SaveCommandJournal(daemon.journal); err != nil {
				daemon.mu.Unlock()
				return err
			}
		}
		daemon.mu.Unlock()
	} else if result != nil && !resultReported {
		var err error
		if command.Kind == "inspect_source" {
			err = daemon.client.SubmitInspectionResult(ctx, command.ID, *result)
		} else {
			err = daemon.client.SubmitResult(ctx, command.ID, *result)
		}
		if err != nil {
			return err
		}
		daemon.mu.Lock()
		if current := daemon.journal.Commands[commandID]; current != nil {
			current.ResultReported = true
			if err := daemon.store.SaveCommandJournal(daemon.journal); err != nil {
				daemon.mu.Unlock()
				return err
			}
		}
		daemon.mu.Unlock()
	}

	daemon.mu.Lock()
	current := daemon.journal.Commands[commandID]
	if current != nil && current.ResultReported && len(current.Events) == 0 && (current.LogID == "" || current.LogCompleted) && len(current.ReplicationPending) == 0 {
		delete(daemon.journal.Commands, commandID)
		err := daemon.store.SaveCommandJournal(daemon.journal)
		daemon.mu.Unlock()
		if err != nil {
			return err
		}
		if logID != "" {
			if err := daemon.store.RemoveRunLog(logID); err != nil {
				return err
			}
		}
		return daemon.store.compactIfPressured()
	}
	daemon.mu.Unlock()
	return nil
}

func (daemon *daemon) reconcileInterrupted(ctx context.Context) error {
	daemon.mu.Lock()
	if daemon.reconciled {
		daemon.mu.Unlock()
		return nil
	}

	ids := make([]string, 0)
	replicationJobs := make(map[string]Job)
	for id, command := range daemon.journal.Commands {
		if command.State == "running" && command.Command.Kind != "update_agent" {
			ids = append(ids, id)
		}
		if command.State == "finished" && len(command.ReplicationPending) > 0 && command.JobSnapshot != "" {
			job, err := decodeJobSnapshot(daemon.bootstrap, command.RunID, command.ConfigRevision, command.JobSnapshot)
			if err == nil {
				replicationJobs[job.ID] = job
			}
		}
	}
	daemon.reconciled = true
	daemon.mu.Unlock()

	for _, id := range ids {
		if err := daemon.reconcileOne(ctx, id); err != nil {
			daemon.mu.Lock()
			daemon.reconciled = false
			daemon.mu.Unlock()
			return err
		}
	}

	for _, job := range replicationJobs {
		daemon.startReplication(ctx, job)
	}

	return nil
}

func (daemon *daemon) restoreAgentUpdateState() error {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()

	commandIDs := make([]string, 0)
	for id, command := range daemon.journal.Commands {
		if command.Command.Kind == "update_agent" && command.State != "finished" {
			commandIDs = append(commandIDs, id)
		}
	}
	sort.Strings(commandIDs)

	if len(commandIDs) == 0 {
		if daemon.state.AgentUpdate == nil {
			return nil
		}
		orphanedCommandID := daemon.state.AgentUpdate.CommandID
		daemon.state.AgentUpdate = nil
		if err := daemon.store.SaveRuntimeState(daemon.state); err != nil {
			return err
		}
		log.Printf("backupchief: cleared orphaned agent update state for command %s", orphanedCommandID)
		return nil
	}

	commandID := commandIDs[0]
	if current := daemon.state.AgentUpdate; current != nil {
		if command := daemon.journal.Commands[current.CommandID]; command != nil && command.Command.Kind == "update_agent" && command.State != "finished" {
			commandID = current.CommandID
		}
	}
	command := daemon.journal.Commands[commandID]
	startedAt := command.ReceivedAt
	lastScheduleMinute := ""
	if current := daemon.state.AgentUpdate; current != nil {
		if current.CommandID == commandID && current.StartedAt != "" {
			startedAt = current.StartedAt
		}
		lastScheduleMinute = current.LastScheduleMinute
	}
	restored := &AgentUpdateRuntime{
		CommandID:          commandID,
		RunID:              command.RunID,
		TargetVersion:      command.Command.Payload.TargetVersion,
		StartedAt:          startedAt,
		LastScheduleMinute: lastScheduleMinute,
	}
	repaired := daemon.state.AgentUpdate == nil || *daemon.state.AgentUpdate != *restored
	daemon.state.AgentUpdate = restored
	if restored.LastScheduleMinute != "" {
		if minute, err := time.Parse("2006-01-02T15:04:05.000000Z", restored.LastScheduleMinute); err == nil {
			daemon.lastScheduleMinute = minute
		}
	}
	if !repaired {
		return nil
	}
	if err := daemon.store.SaveRuntimeState(daemon.state); err != nil {
		return err
	}
	log.Printf("backupchief: restored agent update state for command %s", commandID)
	return nil
}

func (daemon *daemon) startAgentUpdate(commandID string) error {
	daemon.mu.Lock()
	command := daemon.journal.Commands[commandID]
	if command == nil || command.State != "received" || daemon.state.AgentUpdate == nil ||
		len(daemon.active) > 0 || len(daemon.repositories) > 0 || len(daemon.replicationActive) > 0 {
		daemon.mu.Unlock()
		return nil
	}
	target := command.Command.Payload.TargetVersion
	command.State = "running"
	request := updater.Request{
		CommandID: commandID, RunID: command.RunID, Generation: daemon.bootstrap.Generation,
		PreviousVersion: daemon.version, TargetVersion: target, StartedAt: daemon.state.AgentUpdate.StartedAt,
	}
	if err := daemon.store.SaveCommandJournal(daemon.journal); err != nil {
		command.State = "received"
		daemon.mu.Unlock()
		return err
	}
	daemon.mu.Unlock()
	if err := updater.WriteRequest(daemon.store.stateDirectory(), request); err != nil {
		daemon.mu.Lock()
		if current := daemon.journal.Commands[commandID]; current != nil {
			current.State = "received"
			_ = daemon.store.SaveCommandJournal(daemon.journal)
		}
		daemon.mu.Unlock()
		return err
	}
	return nil
}

func (daemon *daemon) consumeAgentUpdateResult() error {
	result, err := updater.ReadResult(daemon.store.stateDirectory())
	if err != nil || result == nil {
		return err
	}

	daemon.mu.Lock()
	runtime := daemon.state.AgentUpdate
	if runtime == nil {
		for _, command := range daemon.journal.Commands {
			if command.UpdateResult != nil && command.UpdateResult.RunID == result.RunID && command.UpdateResult.TargetVersion == result.TargetVersion {
				daemon.mu.Unlock()
				return updater.RemoveResult(daemon.store.stateDirectory())
			}
		}
		daemon.mu.Unlock()
		return fmt.Errorf("updater result has no active update")
	}

	if result.Generation != daemon.bootstrap.Generation || result.RunID != runtime.RunID || result.TargetVersion != runtime.TargetVersion {
		daemon.mu.Unlock()
		return fmt.Errorf("updater result does not match the active update")
	}

	command := daemon.journal.Commands[runtime.CommandID]
	if command == nil || command.Command.Kind != "update_agent" {
		daemon.mu.Unlock()
		return fmt.Errorf("updater result command is unavailable")
	}

	command.State = "finished"
	command.UpdateResult = &AgentUpdateResult{
		Generation: result.Generation, RunID: result.RunID, Status: result.Status, ResultCode: result.ResultCode,
		PreviousVersion: result.PreviousVersion, TargetVersion: result.TargetVersion, InstalledVersion: result.InstalledVersion,
		StartedAt: result.StartedAt, FinishedAt: result.FinishedAt, Diagnostic: result.Diagnostic,
	}
	command.ResultReported = false
	daemon.state.AgentUpdate = nil
	if err := daemon.store.SaveCommandJournal(daemon.journal); err != nil {
		daemon.mu.Unlock()
		return err
	}

	if err := daemon.store.SaveRuntimeState(daemon.state); err != nil {
		daemon.mu.Unlock()
		return err
	}

	daemon.mu.Unlock()
	return updater.RemoveResult(daemon.store.stateDirectory())
}

func (daemon *daemon) reconcileOne(ctx context.Context, commandID string) error {
	daemon.mu.Lock()
	journaled := daemon.journal.Commands[commandID]
	daemon.mu.Unlock()
	if journaled == nil {
		return nil
	}

	job, ready, err := daemon.jobForExecution(ctx, journaled)
	if err != nil {
		return err
	}
	if !ready {
		return fmt.Errorf("required configuration is not available for reconciliation")
	}
	if job == nil {
		return daemon.finishWithoutExecution(commandID, "unresolved", "outcome_unresolved", "The interrupted run configuration is unavailable and it was not rerun.")
	}

	var result CommandResult
	if journaled.RunKind == "backup" {
		request := restic.Request{
			Version:         1,
			Operation:       "snapshots",
			Connection:      job.Repository.Connection,
			Password:        job.Repository.ServicePassword,
			Host:            daemon.bootstrap.ServerID,
			Tags:            []string{"backupchief-job:" + job.ID, "backupchief-run:" + journaled.RunID},
			TimeoutSeconds:  300,
			LockWaitSeconds: 30,
		}
		resticResult := daemon.executor.Run(ctx, request)
		result = reconciledResult(daemon.bootstrap.Generation, journaled, resticResult, daemon.now())
	} else {
		result = daemon.reconciledMaintenance(ctx, journaled, *job)
	}

	if daemon.client != nil && !protocolRevisionSupports(daemon.client.selectedProtocolRevision(), "1.2.0") {
		result.SnapshotEvidence = nil
		result.SnapshotEvidenceIDs = nil
	}
	if daemon.client != nil && !protocolRevisionSupports(daemon.client.selectedProtocolRevision(), "1.4.0") && result.Statistics != nil {
		delete(*result.Statistics, "prune_status")
	}
	result.RepositoryBytes = measureRepositoryBytes(ctx, daemon.executor, *job)

	daemon.mu.Lock()
	if current := daemon.journal.Commands[commandID]; current != nil && current.State == "running" {
		current.State = "finished"
		current.Result = &result
		daemon.appendSnapshotEvidenceEvents(current, result)
		current.Sequence++
		if eventID, idErr := newULID(daemon.now()); idErr == nil {
			current.Events = append(current.Events, daemon.eventEnvelope(
				current, eventID, current.Sequence, result.FinishedAt, "run_finished", terminalEventPayload(result),
			))
		}
		current.ResultReported = current.Trigger == "scheduled"
		daemon.recordOutcomeLocked(*job, result)
		err = daemon.store.SaveCommandJournal(daemon.journal)
	}
	daemon.mu.Unlock()
	return err
}

func (daemon *daemon) reconciledMaintenance(ctx context.Context, command *JournalCommand, job Job) CommandResult {
	timestamp := protocolTimestamp(daemon.now())
	result := CommandResult{
		Generation:  daemon.bootstrap.Generation,
		RunID:       command.RunID,
		JobID:       job.ID,
		RunKind:     command.RunKind,
		Status:      "unresolved",
		ResultCode:  "outcome_unresolved",
		StartedAt:   command.ReceivedAt,
		FinishedAt:  timestamp,
		SnapshotIDs: []string{},
		Summary:     "The interrupted maintenance outcome could not be proved and was not rerun.",
	}

	if command.RunKind != "forget" || command.MaintenancePlan == nil ||
		len(command.MaintenancePlan.CandidateSnapshotIDs) == 0 && !command.MaintenancePlan.ForgetPlanned {
		return result
	}

	request := maintenanceRequest(job)
	request.Operation = "snapshots"
	request.TimeoutSeconds = 300
	request.LockWaitSeconds = 30

	inventoryResult := daemon.executor.Run(ctx, request)
	inventory, ok := parseSnapshotInventory(inventoryResult)
	if !ok {
		attachSnapshotEvidence(&result, "candidates", command.MaintenancePlan.CandidateSnapshotIDs, daemon.now())
		return result
	}

	attachSnapshotEvidence(&result, "repository", snapshotIDs(inventory), daemon.now())

	remaining := 0
	for _, snapshotID := range command.MaintenancePlan.CandidateSnapshotIDs {
		if inventory[snapshotID] {
			remaining++
		}
	}

	removed := len(command.MaintenancePlan.CandidateSnapshotIDs) - remaining
	result.Statistics = maintenanceStatistics(len(inventory)+removed, len(command.MaintenancePlan.CandidateSnapshotIDs), remaining, len(command.MaintenancePlan.ProtectedSnapshotIDs))
	if command.MaintenancePlan.CombinedRetention {
		switch {
		case command.MaintenancePlan.PruneCompleted:
			(*result.Statistics)["prune_status"] = "complete"
			result.PruneCompleted = true
		case command.MaintenancePlan.PruneStarted:
			(*result.Statistics)["prune_status"] = "outcome_unresolved"
			result.Summary = "The interrupted repository prune outcome could not be proved."
			return result
		case command.MaintenancePlan.PruneAfterForget:
			(*result.Statistics)["prune_status"] = "pending"
		default:
			(*result.Statistics)["prune_status"] = "not_due"
		}
	}

	switch {
	case remaining == 0:
		result.Status = "complete"
		result.ResultCode = "success"
		result.Summary = maintenanceSummary("forget", removed)
	case removed > 0:
		result.Status = "partial"
		result.ResultCode = "snapshot_removal_incomplete"
		result.Summary = fmt.Sprintf("Retention removed %d snapshot(s), but %d selected snapshot(s) remain.", removed, remaining)
	default:
		result.Status = "failed"
		result.ResultCode = "execution_failed"
		result.Summary = "The interrupted retention run made no observable snapshot changes and was not repeated."
	}

	return result
}

func reconciledResult(generation uint64, command *JournalCommand, result restic.Result, now time.Time) CommandResult {
	timestamp := protocolTimestamp(now)
	reconciled := CommandResult{
		Generation:  generation,
		RunID:       command.RunID,
		JobID:       command.Command.Payload.JobID,
		RunKind:     "backup",
		Status:      "unresolved",
		ResultCode:  "outcome_unresolved",
		StartedAt:   command.ReceivedAt,
		FinishedAt:  timestamp,
		SnapshotIDs: []string{},
		Summary:     "The interrupted backup outcome could not be proved and was not rerun.",
	}

	if result.ExitCode != 0 {
		return reconciled
	}

	var snapshots []struct {
		ID      string         `json:"id"`
		Summary *resticSummary `json:"summary"`
	}
	if json.Unmarshal([]byte(result.Output), &snapshots) != nil || len(snapshots) == 0 {
		return reconciled
	}

	for _, snapshot := range snapshots {
		if digestPattern.MatchString(snapshot.ID) {
			reconciled.SnapshotIDs = append(reconciled.SnapshotIDs, snapshot.ID)
		}
	}

	last := snapshots[len(snapshots)-1]
	if len(reconciled.SnapshotIDs) == 0 || last.Summary == nil {
		return reconciled
	}

	statistics := RunStatistics{
		"files_new":              last.Summary.FilesNew,
		"files_changed":          last.Summary.FilesChanged,
		"files_unmodified":       last.Summary.FilesUnmodified,
		"directories_new":        last.Summary.DirectoriesNew,
		"directories_changed":    last.Summary.DirectoriesChanged,
		"directories_unmodified": last.Summary.DirectoriesUnmodified,
		"source_files":           last.Summary.SourceFiles,
		"source_bytes":           last.Summary.SourceBytes,
		"stored_bytes":           last.Summary.StoredBytes,
	}

	reconciled.Statistics = &statistics
	reconciled.Status = "complete"
	reconciled.ResultCode = "success"
	reconciled.Summary = "The interrupted backup was reconciled to its tagged snapshot."
	if last.Summary.SourceFiles == 0 {
		reconciled.ResultCode = "empty_selection"
	}

	return reconciled
}
