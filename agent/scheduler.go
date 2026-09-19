package agent

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

const criticalRunRecordEstimate = 64 << 10

var fixedUTCParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

func parseFixedUTCSchedule(expression string) (cron.Schedule, error) {
	fields := strings.Fields(expression)
	if len(fields) != 5 || strings.Join(fields, " ") != expression {
		return nil, fmt.Errorf("schedule must use normalized five-field cron")
	}
	for _, field := range fields {
		for _, character := range field {
			if (character < '0' || character > '9') && !strings.ContainsRune("*,-/", character) {
				return nil, fmt.Errorf("schedule fields must be numeric")
			}
		}
	}
	schedule, err := fixedUTCParser.Parse(expression)
	if err != nil {
		return nil, fmt.Errorf("parse schedule: %w", err)
	}
	baseline := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	previous := schedule.Next(baseline)
	for range 512 {
		next := schedule.Next(previous)
		if next.Sub(previous) < 5*time.Minute {
			return nil, fmt.Errorf("schedule must have at least five minutes between occurrences")
		}
		previous = next
	}
	return schedule, nil
}

func scheduleIsDue(expression string, minute time.Time) (bool, error) {
	schedule, err := parseFixedUTCSchedule(expression)
	if err != nil {
		return false, err
	}
	minute = minute.UTC().Truncate(time.Minute)
	return schedule.Next(minute.Add(-time.Minute)).Equal(minute), nil
}

type scheduledOperation struct {
	Job        Job
	RunKind    string
	Expression string
	Priority   int
	Deferred   bool
}

func (daemon *daemon) scheduleBackups(ctx context.Context) error {
	now := daemon.now().UTC()
	minute := now.Truncate(time.Minute)

	daemon.mu.Lock()
	if !minute.After(daemon.lastScheduleMinute) {
		daemon.lastScheduleMinute = minute
		daemon.mu.Unlock()
		return nil
	}
	previousMinute := daemon.lastScheduleMinute
	daemon.lastScheduleMinute = minute
	draining := daemon.state.AgentUpdate != nil
	if draining {
		daemon.state.AgentUpdate.LastScheduleMinute = protocolTimestamp(minute)
		if err := daemon.store.SaveRuntimeState(daemon.state); err != nil {
			daemon.mu.Unlock()
			return err
		}
	}
	if daemon.state.AuthenticationPaused {
		daemon.mu.Unlock()
		return nil
	}
	config := daemon.config
	activeBackups, activeMaintenance := daemon.activeCountsLocked()
	busyRepositories := make(map[string]bool, len(daemon.repositories))
	for repositoryID, busy := range daemon.repositories {
		busyRepositories[repositoryID] = busy
	}
	daemon.mu.Unlock()

	operations := make([]scheduledOperation, 0, len(config.Jobs)*5)
	for _, job := range config.Jobs {
		if !job.Enabled {
			continue
		}
		operations = append(operations, scheduledOperation{
			Job:        job,
			RunKind:    "backup",
			Expression: job.Schedule.Expression,
			Priority:   0,
		})
		maintenanceOperations := []scheduledOperation{
			{
				Job:        job,
				RunKind:    "forget",
				Expression: job.Retention.ForgetCron,
				Priority:   1,
			},
			{
				Job:        job,
				RunKind:    "prune",
				Expression: job.Retention.PruneCron,
				Priority:   2,
			},
			{
				Job:        job,
				RunKind:    "check_metadata",
				Expression: job.Integrity.MetadataCron,
				Priority:   3,
			},
			{
				Job:        job,
				RunKind:    "check_data",
				Expression: job.Integrity.DataCron,
				Priority:   4,
			},
		}
		if job.Maintenance.Strategy == "after_scheduled_backup" {
			maintenanceOperations = []scheduledOperation{
				{Job: job, RunKind: "forget", Expression: job.Retention.ForgetCron, Priority: -3, Deferred: true},
				{Job: job, RunKind: "check_metadata", Expression: job.Integrity.MetadataCron, Priority: -2, Deferred: true},
				{Job: job, RunKind: "check_data", Expression: job.Integrity.DataCron, Priority: -1, Deferred: true},
			}
		}
		for _, maintenance := range maintenanceOperations {
			if maintenance.Expression != "" {
				operations = append(operations, maintenance)
			}
		}
	}
	sort.Slice(operations, func(first, second int) bool {
		if operations[first].Priority != operations[second].Priority {
			return operations[first].Priority < operations[second].Priority
		}
		return operations[first].Job.ID < operations[second].Job.ID
	})

	for _, operation := range operations {
		scheduleAfter := minute.Add(-time.Minute)
		if draining {
			scheduleAfter = previousMinute
		}
		scheduledMinute, due, err := firstDueSchedule(operation.Expression, scheduleAfter, minute)
		if err != nil {
			return err
		}
		if !due {
			continue
		}
		scheduledFor := protocolTimestamp(scheduledMinute)
		exists, err := daemon.store.occurrenceExists(operation.Job.ID, operation.RunKind, scheduledFor)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		if draining && daemon.hasQueuedScheduledOperation(operation.Job.ID, operation.RunKind, false) {
			if err := daemon.store.recordSkippedOccurrence(operation.Job.ID, operation.RunKind, scheduledFor, nil); err != nil {
				return err
			}
			continue
		}
		if operation.Deferred && daemon.hasQueuedScheduledOperation(operation.Job.ID, operation.RunKind, false) {
			if err := daemon.store.recordSkippedOccurrence(operation.Job.ID, operation.RunKind, scheduledFor, nil); err != nil {
				return err
			}
			continue
		}
		if operation.RunKind == "backup" && daemon.hasQueuedScheduledOperation(operation.Job.ID, operation.RunKind, true) {
			if err := daemon.store.recordSkippedOccurrence(operation.Job.ID, operation.RunKind, scheduledFor, nil); err != nil {
				return err
			}
			continue
		}
		if !daemon.store.canAcceptWork(criticalRunRecordEstimate) {
			if err := daemon.refuseOccurrenceForPressure(operation.Job.ID, operation.RunKind, scheduledFor); err != nil {
				return err
			}
			continue
		}

		runID, err := newULID(now)
		if err != nil {
			return err
		}
		snapshot, err := encodeJobSnapshot(daemon.bootstrap, runID, config.Revision, operation.Job)
		if err != nil {
			return err
		}
		payload := CommandPayload{JobID: operation.Job.ID, RequiredConfigRevision: config.Revision}
		if operation.RunKind != "backup" {
			payload.Maintenance = operation.RunKind
		}
		journaled := &JournalCommand{
			Command: AgentCommand{
				ID: runID, Generation: daemon.bootstrap.Generation, Kind: "scheduled_" + operation.RunKind,
				IssuedAt: scheduledFor, ExpiresAt: protocolTimestamp(minute.Add(24 * time.Hour)), Payload: payload,
			},
			RunID: runID, RunKind: operation.RunKind, ConfigRevision: config.Revision,
			Trigger: "scheduled", ScheduledFor: scheduledFor, JobSnapshot: snapshot,
			ReceivedAt: protocolTimestamp(now), Acknowledged: true, State: "received", Events: []AgentEvent{},
		}
		if operation.Deferred {
			journaled.WaitForBackup = true
			journaled.MaxDeferralAt = protocolTimestamp(minute.Add(time.Duration(operation.Job.Maintenance.MaxDeferralSeconds) * time.Second))
		}

		overlap := busyRepositories[operation.Job.Repository.ID]
		if operation.RunKind == "backup" {
			overlap = overlap || activeBackups >= 2
		} else {
			overlap = overlap || activeMaintenance >= 1
		}
		catchUp := operation.RunKind == "backup" && busyRepositories[operation.Job.Repository.ID] &&
			operation.Job.Maintenance.Strategy == "after_scheduled_backup" && daemon.hasActiveMaintenance(operation.Job.ID)
		if catchUp {
			journaled.CatchUpBackup = true
			overlap = false
		}
		if overlap && !operation.Deferred && !draining {
			daemon.finishScheduledOverlap(journaled, now)
		}
		daemon.mu.Lock()
		daemon.journal.Commands[runID] = journaled
		err = daemon.store.SaveCommandJournal(daemon.journal)
		if err != nil {
			delete(daemon.journal.Commands, runID)
		}
		daemon.mu.Unlock()
		if err != nil {
			if errors.Is(err, ErrSpoolCapacity) {
				if gapErr := daemon.refuseOccurrenceForPressure(operation.Job.ID, operation.RunKind, scheduledFor); gapErr != nil {
					return gapErr
				}
				continue
			}
			return err
		}
		if draining || overlap || operation.Deferred || catchUp {
			continue
		}
		busyRepositories[operation.Job.Repository.ID] = true
		if operation.RunKind == "backup" {
			activeBackups++
		} else {
			activeMaintenance++
		}
		if err := daemon.startOperation(ctx, runID, operation.Job); err != nil {
			return err
		}
	}
	if draining {
		return nil
	}
	return daemon.startNextDeferredMaintenance(ctx, "", true)
}

func firstDueSchedule(expression string, after, through time.Time) (time.Time, bool, error) {
	schedule, err := parseFixedUTCSchedule(expression)
	if err != nil {
		return time.Time{}, false, err
	}
	next := schedule.Next(after.UTC().Truncate(time.Minute))
	return next, !next.After(through.UTC().Truncate(time.Minute)), nil
}

func (daemon *daemon) hasQueuedScheduledOperation(jobID, runKind string, catchUpOnly bool) bool {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	for _, command := range daemon.journal.Commands {
		if command.State != "received" || command.Trigger != "scheduled" || command.Command.Payload.JobID != jobID || command.RunKind != runKind {
			continue
		}
		if catchUpOnly && !command.CatchUpBackup {
			continue
		}
		return true
	}
	return false
}

func (daemon *daemon) hasActiveMaintenance(jobID string) bool {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	for _, command := range daemon.journal.Commands {
		if command.State == "running" && command.RunKind != "backup" && command.Command.Payload.JobID == jobID {
			return true
		}
	}
	return false
}

func (daemon *daemon) startNextDeferredMaintenance(ctx context.Context, jobID string, expiredOnly bool) error {
	daemon.mu.Lock()
	if daemon.state.AgentUpdate != nil {
		daemon.mu.Unlock()
		return nil
	}

	now := daemon.now()
	commandID := ""
	priority := 100
	scheduledFor := ""
	for id, command := range daemon.journal.Commands {
		if command.State != "received" || command.MaxDeferralAt == "" || jobID != "" && command.Command.Payload.JobID != jobID {
			continue
		}
		if expiredOnly {
			maxDeferralAt, err := time.Parse("2006-01-02T15:04:05.000000Z", command.MaxDeferralAt)
			if err != nil || now.Before(maxDeferralAt) {
				continue
			}
		}
		candidatePriority := maintenancePriority(command.RunKind)
		if commandID == "" || candidatePriority < priority || candidatePriority == priority && command.ScheduledFor < scheduledFor {
			commandID = id
			priority = candidatePriority
			scheduledFor = command.ScheduledFor
		}
	}
	if commandID == "" {
		daemon.mu.Unlock()
		return nil
	}

	command := daemon.journal.Commands[commandID]
	wasWaiting := command.WaitForBackup
	command.WaitForBackup = false
	if err := daemon.store.SaveCommandJournal(daemon.journal); err != nil {
		command.WaitForBackup = wasWaiting
		daemon.mu.Unlock()
		return err
	}
	daemon.mu.Unlock()

	job, ready, err := daemon.jobForExecution(ctx, command)
	if err != nil {
		return err
	}
	if !ready {
		return nil
	}

	if job == nil || !job.Enabled {
		return daemon.finishWithoutExecution(commandID, "skipped", "config_unavailable", "The required job configuration is unavailable.")
	}

	return daemon.startOperation(ctx, commandID, *job)
}

func maintenancePriority(runKind string) int {
	switch runKind {
	case "snapshot_inventory":
		return 0
	case "forget":
		return 1
	case "check_metadata":
		return 2
	case "check_data":
		return 3
	default:
		return 4
	}
}

func (daemon *daemon) startCatchUpBackup(ctx context.Context, jobID string) error {
	daemon.mu.Lock()
	if daemon.state.AgentUpdate != nil {
		daemon.mu.Unlock()
		return nil
	}
	commandID := ""
	var command *JournalCommand
	for id, candidate := range daemon.journal.Commands {
		if candidate.State == "received" && candidate.CatchUpBackup && candidate.Command.Payload.JobID == jobID && (command == nil || candidate.ScheduledFor < command.ScheduledFor) {
			commandID = id
			command = candidate
		}
	}
	daemon.mu.Unlock()
	if command == nil {
		return nil
	}
	job, ready, err := daemon.jobForExecution(ctx, command)
	if err != nil || !ready {
		return err
	}
	if job == nil || !job.Enabled {
		return daemon.finishWithoutExecution(commandID, "skipped", "config_unavailable", "The required job configuration is unavailable.")
	}
	return daemon.startOperation(ctx, commandID, *job)
}

func (daemon *daemon) finishScheduledOverlap(command *JournalCommand, now time.Time) {
	timestamp := protocolTimestamp(now)
	command.State = "finished"
	command.ResultReported = true
	command.Result = &CommandResult{
		Generation: daemon.bootstrap.Generation, RunID: command.RunID, JobID: command.Command.Payload.JobID,
		RunKind: command.RunKind, Status: "skipped", ResultCode: "schedule_overlap",
		StartedAt: timestamp, FinishedAt: timestamp, SnapshotIDs: []string{},
		Summary: "The scheduled operation was skipped because execution capacity was unavailable.",
	}
	command.Sequence = 1
	if eventID, err := newULID(now); err == nil {
		command.Events = append(command.Events, daemon.eventEnvelope(
			command, eventID, command.Sequence, timestamp, "run_finished", terminalEventPayload(*command.Result),
		))
	}
}

func (daemon *daemon) refuseOccurrenceForPressure(jobID, runKind, scheduledFor string) error {
	daemon.mu.Lock()
	nextState := daemon.state
	nextState.SpoolGapDetected = true
	nextState.SpoolGapVersion++
	err := daemon.store.recordSkippedOccurrence(jobID, runKind, scheduledFor, &nextState)
	if errors.Is(err, ErrSpoolCapacity) {
		err = daemon.store.recordSkippedOccurrence(jobID, runKind, scheduledFor, &nextState)
	}
	if err != nil {
		daemon.mu.Unlock()
		return err
	}
	daemon.state = nextState
	daemon.mu.Unlock()
	return nil
}

func (daemon *daemon) eventEnvelope(
	command *JournalCommand,
	eventID string,
	sequence uint64,
	occurredAt string,
	kind string,
	payload map[string]any,
) AgentEvent {
	return AgentEvent{
		ID: eventID, RunID: command.RunID, JobID: command.Command.Payload.JobID, RunKind: command.RunKind,
		ConfigRevision: command.ConfigRevision, Trigger: command.Trigger, ScheduledFor: command.ScheduledFor,
		Sequence: sequence, OccurredAt: occurredAt, Kind: kind, Payload: payload,
	}
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
