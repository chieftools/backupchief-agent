package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/chieftools/backupchief-agent/restic"
)

type MaintenancePlan struct {
	Kind                 string   `json:"kind"`
	CandidateSnapshotIDs []string `json:"candidate_snapshot_ids,omitempty"`
	ProtectedSnapshotIDs []string `json:"protected_snapshot_ids,omitempty"`
	DataSubsetPart       uint64   `json:"data_subset_part,omitempty"`
	DataSubsetTotal      uint64   `json:"data_subset_total,omitempty"`
}

type repositorySnapshot struct {
	ID string `json:"id"`
}

type forgetGroup struct {
	Remove []repositorySnapshot `json:"remove"`
}

type checkSummary struct {
	MessageType string `json:"message_type"`
	NumErrors   uint64 `json:"num_errors"`
}

func executeMaintenance(
	ctx context.Context,
	executor BackupExecutor,
	generation uint64,
	command *JournalCommand,
	job Job,
	plan MaintenancePlan,
	persistPlan func(MaintenancePlan) error,
	now func() time.Time,
) (CommandResult, []byte, bool, uint64) {
	startedAt := now()
	result := CommandResult{
		Generation:  generation,
		RunID:       command.RunID,
		JobID:       job.ID,
		RunKind:     command.RunKind,
		StartedAt:   protocolTimestamp(startedAt),
		SnapshotIDs: []string{},
	}

	timeout := maintenanceTimeout(command.RunKind)
	runContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var log bytes.Buffer
	finish := func(result CommandResult) (CommandResult, []byte, bool, uint64) {
		result.FinishedAt = protocolTimestamp(now())
		contents := log.Bytes()
		truncated := false
		dropped := uint64(0)
		if len(contents) > maximumRunLog {
			dropped = uint64(len(contents) - maximumRunLog)
			contents = contents[:maximumRunLog]
			truncated = true
		}
		return result, append([]byte(nil), contents...), truncated, dropped
	}
	run := func(request restic.Request) restic.Result {
		request.TimeoutSeconds = int(timeout / time.Second)
		request.LockWaitSeconds = 5 * 60
		response := executor.Run(runContext, request)
		appendMaintenanceLog(&log, response)
		return response
	}

	request := maintenanceRequest(job)
	switch command.RunKind {
	case "forget":
		if job.Retention.HasUnresolvedRuns {
			result.Status = "skipped"
			result.ResultCode = "unresolved_runs_present"
			result.Summary = "Retention was skipped because this repository has an unresolved run."
			return finish(result)
		}
		proof := job.Retention.LatestComplete
		if proof == nil || len(proof.SnapshotIDs) == 0 {
			result.Status = "skipped"
			result.ResultCode = "recovery_point_unavailable"
			result.Summary = "Retention was skipped because no complete recovery point is available."
			return finish(result)
		}
		return executeForget(runContext, run, request, result, job, proof, plan, persistPlan, finish)
	case "prune":
		if job.Retention.HasUnresolvedRuns {
			result.Status = "skipped"
			result.ResultCode = "unresolved_runs_present"
			result.Summary = "Prune was skipped because this repository has an unresolved run."
			return finish(result)
		}
		request.Operation = "prune"
		response := run(request)
		result = maintenanceResult(result, response, runContext, "Repository prune completed.")
		return finish(result)
	case "check_metadata", "check_data":
		request.Operation = command.RunKind
		if command.RunKind == "check_data" {
			request.DataSubsetPart = int(plan.DataSubsetPart)
			request.DataSubsetTotal = int(plan.DataSubsetTotal)
		}
		response := run(request)
		summary, found := parseCheckSummary(response.Output)
		integrityFailure := response.ExitCode != 0 && response.ExitCode != 10 && response.ExitCode != 11 && response.ExitCode != 12 && response.Outcome != "cancelled"
		if integrityFailure && summary.NumErrors == 0 {
			summary.NumErrors = 1
		}
		statistics := RunStatistics{
			"data_checked": command.RunKind == "check_data",
			"errors_found": summary.NumErrors,
		}
		if command.RunKind == "check_data" {
			statistics["data_subset_part"] = plan.DataSubsetPart
			statistics["data_subset_total"] = plan.DataSubsetTotal
		}
		if summary.NumErrors > 0 || found && integrityFailure {
			result.Status = "failed"
			result.ResultCode = "repository_corrupt"
			result.Statistics = &statistics
			result.Summary = "The repository check found integrity errors."
			return finish(result)
		}
		result = maintenanceResult(result, response, runContext, "Repository check completed.")
		if response.ExitCode == 0 {
			result.Statistics = &statistics
		}
		return finish(result)
	default:
		result.Status = "failed"
		result.ResultCode = "execution_failed"
		result.Summary = "The maintenance operation is unsupported."
		return finish(result)
	}
}

func executeForget(
	ctx context.Context,
	run func(restic.Request) restic.Result,
	request restic.Request,
	result CommandResult,
	job Job,
	proof *CompleteSnapshotProof,
	plan MaintenancePlan,
	persistPlan func(MaintenancePlan) error,
	finish func(CommandResult) (CommandResult, []byte, bool, uint64),
) (CommandResult, []byte, bool, uint64) {
	request.Operation = "snapshots"
	inventoryResult := run(request)
	inventory, ok := parseSnapshotInventory(inventoryResult)
	if !ok {
		result = maintenanceResult(result, inventoryResult, ctx, "Snapshot inventory completed.")
		return finish(result)
	}
	protected := stringSet(proof.SnapshotIDs)
	for snapshotID := range protected {
		if _, exists := inventory[snapshotID]; !exists {
			result.Status = "skipped"
			result.ResultCode = "recovery_point_unavailable"
			result.Summary = "Retention was skipped because the latest complete recovery point is missing."
			return finish(result)
		}
	}

	candidateIDs := append([]string(nil), plan.CandidateSnapshotIDs...)
	if plan.Kind == "" {
		policy := restic.Retention{
			Last: job.Retention.Last, Hourly: job.Retention.Hourly, Daily: job.Retention.Daily,
			Weekly: job.Retention.Weekly, Monthly: job.Retention.Monthly, Yearly: job.Retention.Yearly,
		}
		if retentionPolicyEmpty(policy) {
			for snapshotID := range inventory {
				candidateIDs = append(candidateIDs, snapshotID)
			}
			sort.Strings(candidateIDs)
		} else {
			request.Operation = "forget_plan"
			request.Retention = &policy
			planResult := run(request)
			var parsed bool
			candidateIDs, parsed = parseForgetCandidates(planResult)
			if !parsed {
				result = maintenanceResult(result, planResult, ctx, "Retention plan completed.")
				return finish(result)
			}
		}
		candidateIDs = withoutProtected(candidateIDs, protected)
		plan = MaintenancePlan{
			Kind: "forget", CandidateSnapshotIDs: candidateIDs,
			ProtectedSnapshotIDs: append([]string(nil), proof.SnapshotIDs...),
		}
		if err := persistPlan(plan); err != nil {
			result.Status = "failed"
			result.ResultCode = "execution_failed"
			result.Summary = "The retention plan could not be persisted before execution."
			return finish(result)
		}
	}

	if len(candidateIDs) == 0 {
		result.Status = "complete"
		result.ResultCode = "success"
		result.Statistics = maintenanceStatistics(len(inventory), 0, 0, len(protected))
		result.Summary = "Retention found no snapshots to remove."
		return finish(result)
	}
	considered := len(inventory)
	for _, snapshotID := range candidateIDs {
		if !inventory[snapshotID] {
			considered++
		}
	}

	var failed *restic.Result
	for offset := 0; offset < len(candidateIDs); offset += 100 {
		end := min(offset+100, len(candidateIDs))
		request.Operation = "forget"
		request.Retention = nil
		request.SnapshotIDs = candidateIDs[offset:end]
		response := run(request)
		if response.ExitCode != 0 {
			failed = &response
			break
		}
	}

	request.Operation = "snapshots"
	request.SnapshotIDs = nil
	afterResult := run(request)
	after, listed := parseSnapshotInventory(afterResult)
	if !listed {
		result.Status = "unresolved"
		result.ResultCode = "outcome_unresolved"
		result.Summary = "Retention changed repository metadata but its final snapshot state could not be verified."
		return finish(result)
	}
	remaining := 0
	for _, snapshotID := range candidateIDs {
		if after[snapshotID] {
			remaining++
		}
	}
	removed := len(candidateIDs) - remaining
	result.Statistics = maintenanceStatistics(considered, len(candidateIDs), remaining, len(protected))
	if remaining == 0 {
		result.Status = "complete"
		result.ResultCode = "success"
		result.Summary = maintenanceSummary("forget", removed)
		return finish(result)
	}
	if removed > 0 {
		result.Status = "partial"
		result.ResultCode = "snapshot_removal_incomplete"
		result.Summary = fmt.Sprintf("Retention removed %d snapshot(s), but %d selected snapshot(s) remain.", removed, remaining)
		return finish(result)
	}
	if failed != nil {
		result = maintenanceResult(result, *failed, ctx, "Retention completed.")
		result.Statistics = maintenanceStatistics(considered, len(candidateIDs), remaining, len(protected))
		return finish(result)
	}
	result.Status = "failed"
	result.ResultCode = "execution_failed"
	result.Summary = "Retention did not remove its selected snapshots."
	return finish(result)
}
func maintenanceRequest(job Job) restic.Request {
	return restic.Request{
		Version: 1,
		Connection: restic.Connection{
			Driver:    job.Repository.Connection.Driver,
			Path:      job.Repository.Connection.Path,
			Endpoint:  job.Repository.Connection.Endpoint,
			Bucket:    job.Repository.Connection.Bucket,
			Prefix:    job.Repository.Connection.Prefix,
			Region:    job.Repository.Connection.Region,
			AccessKey: job.Repository.Connection.AccessKey,
			SecretKey: job.Repository.Connection.SecretKey,
		},
		Password: job.Repository.ServicePassword,
	}
}

func maintenanceTimeout(kind string) time.Duration {
	switch kind {
	case "forget":
		return 30 * time.Minute
	case "prune":
		return 6 * time.Hour
	case "check_metadata":
		return time.Hour
	default:
		return 12 * time.Hour
	}
}

func maintenanceResult(base CommandResult, response restic.Result, ctx context.Context, successSummary string) CommandResult {
	if response.ExitCode == 0 {
		base.Status = "complete"
		base.ResultCode = "success"
		base.Summary = successSummary
		return base
	}
	if response.Outcome == "cancelled" {
		if ctx.Err() == context.DeadlineExceeded {
			base.Status = "failed"
			base.ResultCode = "timed_out"
			base.Summary = "The maintenance operation exceeded its execution deadline."
		} else {
			base.Status = "cancelled"
			base.ResultCode = "cancelled"
			base.Summary = "The maintenance operation was cancelled."
		}
		return base
	}
	base.Status = "failed"
	base.ResultCode = resultCodeForExit(response.ExitCode)
	base.Summary = "Restic could not complete the maintenance operation."
	return base
}

func parseSnapshotInventory(result restic.Result) (map[string]bool, bool) {
	if result.ExitCode != 0 {
		return nil, false
	}
	var snapshots []repositorySnapshot
	if json.Unmarshal([]byte(result.Output), &snapshots) != nil {
		return nil, false
	}
	inventory := make(map[string]bool, len(snapshots))
	for _, snapshot := range snapshots {
		if !digestPattern.MatchString(snapshot.ID) {
			return nil, false
		}
		inventory[snapshot.ID] = true
	}
	return inventory, true
}

func parseForgetCandidates(result restic.Result) ([]string, bool) {
	if result.ExitCode != 0 {
		return nil, false
	}
	var groups []forgetGroup
	if json.Unmarshal([]byte(result.Output), &groups) != nil {
		return nil, false
	}
	seen := make(map[string]bool)
	for _, group := range groups {
		for _, snapshot := range group.Remove {
			if !digestPattern.MatchString(snapshot.ID) {
				return nil, false
			}
			seen[snapshot.ID] = true
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, true
}

func parseCheckSummary(output string) (checkSummary, bool) {
	var found checkSummary
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		var candidate checkSummary
		if json.Unmarshal(scanner.Bytes(), &candidate) == nil && candidate.MessageType == "summary" {
			found = candidate
		}
	}
	return found, found.MessageType == "summary"
}

func appendMaintenanceLog(log *bytes.Buffer, result restic.Result) {
	if result.Output != "" {
		log.WriteString(result.Output)
		if !strings.HasSuffix(result.Output, "\n") {
			log.WriteByte('\n')
		}
	}
	if result.Diagnostic != "" {
		log.WriteString(result.Diagnostic)
		if !strings.HasSuffix(result.Diagnostic, "\n") {
			log.WriteByte('\n')
		}
	}
}

func stringSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}

func retentionPolicyEmpty(policy restic.Retention) bool {
	return policy.Last == 0 && policy.Hourly == 0 && policy.Daily == 0 && policy.Weekly == 0 && policy.Monthly == 0 && policy.Yearly == 0
}

func withoutProtected(values []string, protected map[string]bool) []string {
	filtered := make([]string, 0, len(values))
	for _, value := range values {
		if !protected[value] {
			filtered = append(filtered, value)
		}
	}
	return filtered
}

func maintenanceStatistics(inventory, candidates, remaining, protected int) *RunStatistics {
	statistics := RunStatistics{
		"snapshots_considered": uint64(inventory),
		"snapshots_removed":    uint64(candidates - remaining),
		"snapshots_retained":   uint64(inventory - candidates + remaining),
		"snapshots_protected":  uint64(protected),
	}
	return &statistics
}

func maintenanceSummary(kind string, removed int) string {
	if kind == "forget" {
		return fmt.Sprintf("Retention removed %d snapshot(s).", removed)
	}
	return "Maintenance completed."
}
