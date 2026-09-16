package agent

import (
	"bytes"
	"context"
	"time"
)

func executeMaintenanceRepositories(
	ctx context.Context,
	executor BackupExecutor,
	generation uint64,
	command *JournalCommand,
	job Job,
	plan *MaintenancePlan,
	persistPlan func(MaintenancePlan) error,
	now func() time.Time,
	retryDelays []time.Duration,
) (CommandResult, []byte, bool, uint64) {
	var currentPlan *MaintenancePlan
	if plan != nil {
		copy := *plan
		currentPlan = &copy
	}
	capturePlan := func(updated MaintenancePlan) error {
		copy := updated
		currentPlan = &copy
		return persistPlan(updated)
	}

	primary, primaryLog, truncated, dropped, primaryAttempts := executeRepositoryMaintenance(
		ctx, executor, generation, command, job, func() *MaintenancePlan { return currentPlan },
		capturePlan, now, retryDelays,
	)
	primary.RepositoryBytes = measureRepositoryBytes(ctx, executor, job)
	primary.MaintenancePlan = currentPlan
	primary.RepositoryResults = []RepositoryResult{repositoryResult(job.Repository, primary, primaryAttempts)}
	primary.Diagnostic = ""
	var log bytes.Buffer
	log.Write(primaryLog)

	if command.RunKind == "forget" && primary.Status != "complete" {
		return primary, log.Bytes(), truncated, dropped
	}

	allComplete := primary.Status == "complete"
	pruneCompleted := primary.PruneCompleted
	for _, repository := range job.Replicas {
		childJob := job
		childJob.Repository = repository
		childJob.Replicas = nil
		childJob.ReplicaSetups = nil

		var childPlan *MaintenancePlan
		if currentPlan != nil {
			copy := *currentPlan
			copy.CandidateSnapshotIDs = nil
			copy.ProtectedSnapshotIDs = nil
			childPlan = &copy
		}
		child, childLog, childTruncated, childDropped, childAttempts := executeRepositoryMaintenance(
			ctx, executor, generation, command, childJob, func() *MaintenancePlan { return childPlan },
			func(MaintenancePlan) error { return nil }, now, retryDelays,
		)
		child.RepositoryBytes = measureRepositoryBytes(ctx, executor, childJob)
		primary.RepositoryResults = append(primary.RepositoryResults, repositoryResult(repository, child, childAttempts))
		appendBoundedMaintenanceLog(&log, childLog, &truncated, &dropped)
		truncated = truncated || childTruncated
		dropped += childDropped
		allComplete = allComplete && child.Status == "complete"
		pruneCompleted = pruneCompleted && child.PruneCompleted
	}

	primary.PruneCompleted = pruneCompleted
	primary.MaintenancePlan = currentPlan
	if !allComplete && primary.Status == "complete" {
		primary.Status = "partial"
		primary.ResultCode = "repository_operations_incomplete"
		primary.Summary = "The operation completed on the primary repository, but one or more replicas need attention."
	}
	return primary, log.Bytes(), truncated, dropped
}

func executeRepositoryMaintenance(
	ctx context.Context,
	executor BackupExecutor,
	generation uint64,
	command *JournalCommand,
	job Job,
	plan func() *MaintenancePlan,
	persistPlan func(MaintenancePlan) error,
	now func() time.Time,
	retryDelays []time.Duration,
) (CommandResult, []byte, bool, uint64, uint64) {
	var result CommandResult
	var log bytes.Buffer
	truncated := false
	dropped := uint64(0)
	attempts := uint64(0)

	for {
		attempts++
		attemptResult, attemptLog, attemptTruncated, attemptDropped := executeMaintenance(
			ctx, executor, generation, command, job, plan(), persistPlan, now,
		)
		result = attemptResult
		appendBoundedMaintenanceLog(&log, attemptLog, &truncated, &dropped)
		truncated = truncated || attemptTruncated
		dropped += attemptDropped

		if !retryableMaintenanceResult(result) || int(attempts) > len(retryDelays) {
			break
		}

		timer := time.NewTimer(retryDelays[attempts-1])
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, log.Bytes(), truncated, dropped, attempts
		case <-timer.C:
		}
	}

	return result, log.Bytes(), truncated, dropped, attempts
}

func retryableMaintenanceResult(result CommandResult) bool {
	return result.Status == "failed" && contains(
		[]string{"repository_locked", "timed_out", "execution_failed"},
		result.ResultCode,
	)
}

func appendBoundedMaintenanceLog(log *bytes.Buffer, contents []byte, truncated *bool, dropped *uint64) {
	available := maximumRunLog - log.Len()
	if available <= 0 {
		*dropped += uint64(len(contents))
		*truncated = *truncated || len(contents) > 0
		return
	}
	if len(contents) > available {
		log.Write(contents[:available])
		*dropped += uint64(len(contents) - available)
		*truncated = true
		return
	}
	log.Write(contents)
}

func repositoryResult(repository JobRepository, result CommandResult, attemptCount uint64) RepositoryResult {
	return RepositoryResult{
		RepositoryKey: repository.Key, RepositoryID: repository.ID,
		Status: result.Status, ResultCode: result.ResultCode, AttemptCount: attemptCount,
		StartedAt: result.StartedAt, FinishedAt: result.FinishedAt, Summary: result.Summary,
		Diagnostic: result.Diagnostic,
		Statistics: result.Statistics, RepositoryBytes: result.RepositoryBytes,
		SnapshotEvidence:    result.SnapshotEvidence,
		SnapshotEvidenceIDs: append([]string(nil), result.SnapshotEvidenceIDs...),
	}
}
