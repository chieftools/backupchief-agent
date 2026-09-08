package agent

import "context"

func (daemon *daemon) activeCountsLocked() (int, int) {
	backups := 0
	maintenance := 0
	for _, runKind := range daemon.activeRunKinds {
		if runKind == "backup" {
			backups++
		} else {
			maintenance++
		}
	}
	return backups, maintenance
}

func (daemon *daemon) prepareMaintenanceLocked(job Job, journaled *JournalCommand) (Job, MaintenancePlan) {
	if daemon.state.Maintenance == nil {
		daemon.state.Maintenance = make(map[string]MaintenanceRuntime)
	}
	runtimeState := daemon.state.Maintenance[job.Repository.ID]
	if newerCompleteProof(job.Retention.LatestComplete, runtimeState.LatestComplete) {
		runtimeState.LatestComplete = cloneCompleteProof(job.Retention.LatestComplete)
	}
	if runtimeState.LatestComplete != nil {
		job.Retention.LatestComplete = cloneCompleteProof(runtimeState.LatestComplete)
	}
	job.Retention.HasUnresolvedRuns = job.Retention.HasUnresolvedRuns || runtimeState.Unresolved

	plan := MaintenancePlan{Kind: journaled.RunKind}
	if journaled.MaintenancePlan != nil {
		plan = *journaled.MaintenancePlan
	}
	if journaled.RunKind == "check_data" && journaled.MaintenancePlan == nil {
		if runtimeState.DataParts != job.Integrity.DataParts {
			runtimeState.DataParts = job.Integrity.DataParts
			runtimeState.NextDataPart = 1
		}
		if runtimeState.NextDataPart < 1 || runtimeState.NextDataPart > job.Integrity.DataParts {
			runtimeState.NextDataPart = 1
		}
		plan.DataSubsetPart = runtimeState.NextDataPart
		plan.DataSubsetTotal = job.Integrity.DataParts
		journaled.MaintenancePlan = &plan
	}
	if job.Retention.HasUnresolvedRuns {
		runtimeState.Unresolved = true
	}
	daemon.state.Maintenance[job.Repository.ID] = runtimeState
	return job, plan
}

func (daemon *daemon) recordOutcomeLocked(job Job, result CommandResult) {
	if daemon.state.Maintenance == nil {
		daemon.state.Maintenance = make(map[string]MaintenanceRuntime)
	}
	runtimeState := daemon.state.Maintenance[job.Repository.ID]
	if result.RunKind == "backup" && result.Status == "complete" && len(result.SnapshotIDs) > 0 {
		runtimeState.LatestComplete = &CompleteSnapshotProof{
			RunID: result.RunID, FinishedAt: result.FinishedAt, SnapshotIDs: append([]string(nil), result.SnapshotIDs...),
		}
	}
	if result.Status == "unresolved" {
		runtimeState.Unresolved = true
		runtimeState.UnresolvedConfirmed = false
		runtimeState.UnresolvedAtRevision = daemon.metadata.Revision
	}
	if result.RunKind == "check_data" && result.Status == "complete" && result.ResultCode == "success" {
		parts := job.Integrity.DataParts
		part := uint64(1)
		if runtimeState.DataParts == parts && runtimeState.NextDataPart >= 1 && runtimeState.NextDataPart <= parts {
			part = runtimeState.NextDataPart
		}
		runtimeState.DataParts = parts
		runtimeState.NextDataPart = part%parts + 1
	}
	daemon.state.Maintenance[job.Repository.ID] = runtimeState
	_ = daemon.store.SaveRuntimeState(daemon.state)
}

func newerCompleteProof(candidate, current *CompleteSnapshotProof) bool {
	if candidate == nil {
		return false
	}
	if current == nil {
		return true
	}
	if candidate.FinishedAt != current.FinishedAt {
		return candidate.FinishedAt > current.FinishedAt
	}
	return candidate.RunID > current.RunID
}

func cloneCompleteProof(proof *CompleteSnapshotProof) *CompleteSnapshotProof {
	if proof == nil {
		return nil
	}
	return &CompleteSnapshotProof{
		RunID: proof.RunID, FinishedAt: proof.FinishedAt, SnapshotIDs: append([]string(nil), proof.SnapshotIDs...),
	}
}

func (daemon *daemon) acceptMaintenanceConfigLocked(config Config) ([]context.CancelFunc, []string) {
	if daemon.state.Maintenance == nil {
		daemon.state.Maintenance = make(map[string]MaintenanceRuntime)
	}
	availableJobs := make(map[string]bool, len(config.Jobs))
	for index := range config.Jobs {
		job := &config.Jobs[index]
		if !job.Enabled {
			continue
		}
		availableJobs[job.ID] = true
		runtimeState := daemon.state.Maintenance[job.Repository.ID]
		if newerCompleteProof(job.Retention.LatestComplete, runtimeState.LatestComplete) {
			runtimeState.LatestComplete = cloneCompleteProof(job.Retention.LatestComplete)
		}
		if job.Retention.HasUnresolvedRuns {
			runtimeState.Unresolved = true
			runtimeState.UnresolvedConfirmed = true
			runtimeState.UnresolvedAtRevision = config.Revision
		} else if runtimeState.Unresolved && runtimeState.UnresolvedConfirmed && config.Revision > runtimeState.UnresolvedAtRevision {
			runtimeState.Unresolved = false
			runtimeState.UnresolvedConfirmed = false
			runtimeState.UnresolvedAtRevision = 0
		}
		if runtimeState.DataParts != 0 && runtimeState.DataParts != job.Integrity.DataParts {
			runtimeState.DataParts = job.Integrity.DataParts
			runtimeState.NextDataPart = 1
		}
		daemon.state.Maintenance[job.Repository.ID] = runtimeState
	}

	cancellations := make([]context.CancelFunc, 0)
	queued := make([]string, 0)
	for commandID, command := range daemon.journal.Commands {
		if command.RunKind == "" || command.RunKind == "backup" || availableJobs[command.Command.Payload.JobID] {
			continue
		}
		if command.State == "running" {
			if cancel := daemon.active[command.RunID]; cancel != nil {
				cancellations = append(cancellations, cancel)
			}
		} else if command.State == "received" {
			queued = append(queued, commandID)
		}
	}
	return cancellations, queued
}
