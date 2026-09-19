package agent

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
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
	CandidateRunIDs      []string `json:"candidate_run_ids,omitempty"`
	ProtectedRunIDs      []string `json:"protected_run_ids,omitempty"`
	DataSubsetPart       uint64   `json:"data_subset_part,omitempty"`
	DataSubsetTotal      uint64   `json:"data_subset_total,omitempty"`
	CombinedRetention    bool     `json:"combined_retention,omitempty"`
	ForgetPlanned        bool     `json:"forget_planned,omitempty"`
	PruneAfterForget     bool     `json:"prune_after_forget,omitempty"`
	PruneStarted         bool     `json:"prune_started,omitempty"`
	PruneCompleted       bool     `json:"prune_completed,omitempty"`
}

type repositorySnapshot struct {
	ID       string   `json:"id"`
	Original string   `json:"original"`
	Tags     []string `json:"tags"`
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
	plan *MaintenancePlan,
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

	timeout := maintenanceTimeout(command.RunKind, job)
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
	if plan != nil && (plan.Kind == "" || plan.Kind != command.RunKind) {
		result.Status = "failed"
		result.ResultCode = "execution_failed"
		result.Summary = "The persisted maintenance plan does not match this operation."
		return finish(result)
	}
	maintenancePrepared := false
	run := func(request restic.Request) restic.Result {
		request.TimeoutSeconds = int(timeout / time.Second)
		request.LockWaitSeconds = 5 * 60
		request.RecoverStaleLocks = !maintenancePrepared
		maintenancePrepared = true
		response := executor.Run(runContext, request)
		appendMaintenanceLog(&log, response)
		if response.ExitCode == 11 && !request.RecoverStaleLocks {
			request.RecoverStaleLocks = true
			response = executor.Run(runContext, request)
			appendMaintenanceLog(&log, response)
		}
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
		return executeForget(runContext, run, request, result, job, proof, command.Command.Payload.SnapshotIDs, command.Command.Payload.RecoveryPointRunIDs, plan, persistPlan, finish, now)
	case "snapshot_inventory":
		request.Operation = "snapshots"
		response := run(request)
		inventory, ok := parseSnapshotInventory(response)
		if !ok {
			result = maintenanceResult(result, response, runContext, "Snapshot inventory completed.")
			return finish(result)
		}
		attachSnapshotEvidence(&result, "repository", snapshotIDs(inventory), now())
		statistics := RunStatistics{"snapshots_reported": len(inventory)}
		result.Status = "complete"
		result.ResultCode = "success"
		result.Statistics = &statistics
		result.Summary = "Snapshot inventory verified."
		return finish(result)
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
			if plan == nil {
				result.Status = "failed"
				result.ResultCode = "execution_failed"
				result.Summary = "The data check plan is missing."
				return finish(result)
			}
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
	targetSnapshotIDs []string,
	targetRunIDs []string,
	plan *MaintenancePlan,
	persistPlan func(MaintenancePlan) error,
	finish func(CommandResult) (CommandResult, []byte, bool, uint64),
	now func() time.Time,
) (CommandResult, []byte, bool, uint64) {
	request.Operation = "snapshots"
	inventoryResult := run(request)
	inventory, ok := parseSnapshotInventory(inventoryResult)
	if !ok {
		result = maintenanceResult(result, inventoryResult, ctx, "Snapshot inventory completed.")
		return finish(result)
	}
	inventoryObservedAt := now()
	records := snapshotRecords(inventoryResult)
	proofSnapshotIDs := snapshotIDsForRunIDs(records, []string{proof.RunID})
	if len(proofSnapshotIDs) == 0 {
		proofSnapshotIDs = repositorySnapshotIDs(records, proof.SnapshotIDs)
	}
	protected := stringSet(proofSnapshotIDs)
	for _, snapshotID := range repositorySnapshotIDs(records, job.Retention.ProtectedSnapshotIDs) {
		protected[snapshotID] = true
	}
	for _, snapshotID := range snapshotIDsForRunIDs(records, job.Retention.ProtectedRunIDs) {
		protected[snapshotID] = true
	}
	for snapshotID := range protected {
		if _, exists := inventory[snapshotID]; !exists {
			attachSnapshotEvidence(&result, "repository", snapshotIDs(inventory), inventoryObservedAt)
			result.Status = "skipped"
			result.ResultCode = "recovery_point_unavailable"
			result.Summary = "Retention was skipped because the latest complete recovery point is missing."
			return finish(result)
		}
	}

	candidateIDs := make([]string, 0)
	forgetPlanned := plan != nil && (plan.ForgetPlanned || len(plan.CandidateSnapshotIDs) > 0 || len(plan.ProtectedSnapshotIDs) > 0)
	if forgetPlanned {
		if len(plan.CandidateRunIDs) > 0 {
			candidateIDs = snapshotIDsForRunIDs(records, plan.CandidateRunIDs)
		} else {
			candidateIDs = append(candidateIDs, plan.CandidateSnapshotIDs...)
		}
	}
	if !forgetPlanned {
		if len(targetRunIDs) > 0 {
			candidateIDs = snapshotIDsForRunIDs(records, targetRunIDs)
		} else if len(targetSnapshotIDs) > 0 {
			candidateIDs = append(candidateIDs, targetSnapshotIDs...)
			if intersectsProtected(candidateIDs, protected) {
				attachSnapshotEvidence(&result, "repository", snapshotIDs(inventory), inventoryObservedAt)
				result.Status = "skipped"
				result.ResultCode = "recovery_point_protected"
				result.Summary = "The recovery point was not expired because it is protected."
				return finish(result)
			}
			if isDatabaseJob(job.Type) {
				candidateIDs = expandDatabaseRunCandidates(snapshotRecords(inventoryResult), candidateIDs)
			}
			if intersectsProtected(candidateIDs, protected) {
				attachSnapshotEvidence(&result, "repository", snapshotIDs(inventory), inventoryObservedAt)
				result.Status = "skipped"
				result.ResultCode = "recovery_point_protected"
				result.Summary = "The recovery point was not expired because it is protected."
				return finish(result)
			}
			present := map[string]bool{}
			for _, snapshotID := range candidateIDs {
				if inventory[snapshotID] {
					present[snapshotID] = true
				}
			}
			candidateIDs = snapshotIDs(present)
		} else {
			policy := restic.Retention{
				Last: job.Retention.Last, Hourly: job.Retention.Hourly, Daily: job.Retention.Daily,
				Weekly: job.Retention.Weekly, Monthly: job.Retention.Monthly, Yearly: job.Retention.Yearly,
			}
			if retentionPolicyEmpty(policy) {
				for _, snapshot := range snapshotRecords(inventoryResult) {
					if !isDatabaseJob(job.Type) || contains(snapshot.Tags, "backupchief-run-anchor") {
						candidateIDs = append(candidateIDs, snapshot.ID)
					}
				}
				sort.Strings(candidateIDs)
			} else {
				request.Operation = "forget_plan"
				request.Retention = &policy
				if isDatabaseJob(job.Type) {
					request.Tags = []string{"backupchief-run-anchor"}
				}
				planResult := run(request)
				var parsed bool
				candidateIDs, parsed = parseForgetCandidates(planResult)
				if !parsed {
					result = maintenanceResult(result, planResult, ctx, "Retention plan completed.")
					return finish(result)
				}
			}
			if isDatabaseJob(job.Type) {
				candidateIDs = expandDatabaseRunCandidates(snapshotRecords(inventoryResult), candidateIDs)
			}
			candidateIDs = withoutProtected(candidateIDs, protected)
		}
		persistedPlan := MaintenancePlan{
			Kind: "forget", CandidateSnapshotIDs: candidateIDs,
			ProtectedSnapshotIDs: snapshotIDs(protected),
			CandidateRunIDs:      runIDsForSnapshots(records, candidateIDs),
			ProtectedRunIDs:      append([]string{proof.RunID}, job.Retention.ProtectedRunIDs...),
			ForgetPlanned:        true,
		}
		if plan != nil {
			persistedPlan.CombinedRetention = plan.CombinedRetention
			persistedPlan.PruneAfterForget = plan.PruneAfterForget
		}
		if err := persistPlan(persistedPlan); err != nil {
			result.Status = "failed"
			result.ResultCode = "execution_failed"
			result.Summary = "The retention plan could not be persisted before execution."
			return finish(result)
		}
		plan = &persistedPlan
	}

	if len(candidateIDs) == 0 {
		attachSnapshotEvidence(&result, "repository", snapshotIDs(inventory), inventoryObservedAt)
		result.Status = "complete"
		result.ResultCode = "success"
		result.Statistics = maintenanceStatistics(len(inventory), 0, 0, len(protected))
		result.Summary = "Retention found no snapshots to remove."
		return finishRetention(ctx, run, request, result, plan, persistPlan, finish)
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
		attachSnapshotEvidence(&result, "candidates", candidateIDs, now())
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
	attachSnapshotEvidence(&result, "repository", snapshotIDs(after), now())
	result.Statistics = maintenanceStatistics(considered, len(candidateIDs), remaining, len(protected))
	if remaining == 0 {
		result.Status = "complete"
		result.ResultCode = "success"
		result.Summary = maintenanceSummary("forget", removed)
		return finishRetention(ctx, run, request, result, plan, persistPlan, finish)
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

func finishRetention(
	ctx context.Context,
	run func(restic.Request) restic.Result,
	request restic.Request,
	result CommandResult,
	plan *MaintenancePlan,
	persistPlan func(MaintenancePlan) error,
	finish func(CommandResult) (CommandResult, []byte, bool, uint64),
) (CommandResult, []byte, bool, uint64) {
	if plan == nil || !plan.CombinedRetention {
		return finish(result)
	}
	if result.Statistics == nil {
		statistics := RunStatistics{}
		result.Statistics = &statistics
	}
	if !plan.PruneAfterForget {
		(*result.Statistics)["prune_status"] = "not_due"
		return finish(result)
	}

	updated := *plan
	updated.PruneStarted = true
	if err := persistPlan(updated); err != nil {
		result.Status = "failed"
		result.ResultCode = "execution_failed"
		result.Summary = "Retention completed, but the prune phase could not be persisted before execution."
		(*result.Statistics)["prune_status"] = "failed"
		return finish(result)
	}

	request.Operation = "prune"
	request.Retention = nil
	request.SnapshotIDs = nil
	request.Tags = nil
	response := run(request)
	if response.ExitCode != 0 {
		result = maintenanceResult(result, response, ctx, "Repository prune completed.")
		(*result.Statistics)["prune_status"] = pruneStatus(response, ctx)
		result.Summary = "Retention completed, but repository prune did not complete."
		return finish(result)
	}

	updated.PruneCompleted = true
	if err := persistPlan(updated); err != nil {
		result.Status = "unresolved"
		result.ResultCode = "outcome_unresolved"
		result.Summary = "Repository prune completed, but its durable completion marker could not be saved."
		(*result.Statistics)["prune_status"] = "outcome_unresolved"
		return finish(result)
	}
	result.PruneCompleted = true
	(*result.Statistics)["prune_status"] = "complete"
	result.Summary = "Retention and repository prune completed."
	return finish(result)
}

func pruneStatus(response restic.Result, ctx context.Context) string {
	if response.Outcome != "cancelled" {
		return "failed"
	}
	if ctx.Err() == context.DeadlineExceeded {
		return "timed_out"
	}
	return "cancelled"
}

func snapshotIDs(inventory map[string]bool) []string {
	result := make([]string, 0, len(inventory))
	for snapshotID := range inventory {
		result = append(result, snapshotID)
	}
	sort.Strings(result)
	return result
}

func repositorySnapshotIDs(snapshots []repositorySnapshot, snapshotIDs []string) []string {
	localIDs := make(map[string]string, len(snapshots)*2)
	for _, snapshot := range snapshots {
		localIDs[snapshot.ID] = snapshot.ID
		if digestPattern.MatchString(snapshot.Original) {
			localIDs[snapshot.Original] = snapshot.ID
		}
	}

	result := make([]string, 0, len(snapshotIDs))
	for _, snapshotID := range snapshotIDs {
		if localID := localIDs[snapshotID]; localID != "" {
			result = append(result, localID)
		} else {
			result = append(result, snapshotID)
		}
	}
	return result
}

func attachSnapshotEvidence(result *CommandResult, scope string, snapshotIDs []string, observedAt time.Time) {
	ids := append([]string(nil), snapshotIDs...)
	sort.Strings(ids)
	digestInput := strings.Join(ids, "\n")
	if len(ids) > 0 {
		digestInput += "\n"
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(digestInput)))
	result.SnapshotEvidence = &SnapshotEvidence{
		Scope: scope, ObservedAt: protocolTimestamp(observedAt), SnapshotCount: len(ids),
		ChunkCount: (len(ids) + snapshotEvidenceChunkSize - 1) / snapshotEvidenceChunkSize, SHA256: digest,
	}
	result.SnapshotEvidenceIDs = ids
}

func snapshotRecords(result restic.Result) []repositorySnapshot {
	var snapshots []repositorySnapshot
	if result.ExitCode != 0 || json.Unmarshal([]byte(result.Output), &snapshots) != nil {
		return nil
	}
	return snapshots
}

func expandDatabaseRunCandidates(snapshots []repositorySnapshot, anchors []string) []string {
	anchorSet := stringSet(anchors)
	runTags := map[string]bool{}
	for _, snapshot := range snapshots {
		if !anchorSet[snapshot.ID] {
			continue
		}
		for _, tag := range snapshot.Tags {
			if strings.HasPrefix(tag, "backupchief-run:") {
				runTags[tag] = true
			}
		}
	}
	result := []string{}
	for _, snapshot := range snapshots {
		for _, tag := range snapshot.Tags {
			if runTags[tag] {
				result = append(result, snapshot.ID)
				break
			}
		}
	}
	sort.Strings(result)
	return result
}

func snapshotIDsForRunIDs(snapshots []repositorySnapshot, runIDs []string) []string {
	runTags := make(map[string]bool, len(runIDs))
	for _, runID := range runIDs {
		runTags["backupchief-run:"+strings.TrimPrefix(runID, "run_")] = true
	}
	result := make([]string, 0)
	for _, snapshot := range snapshots {
		for _, tag := range snapshot.Tags {
			if runTags[tag] {
				result = append(result, snapshot.ID)
				break
			}
		}
	}
	sort.Strings(result)
	return result
}

func runIDsForSnapshots(snapshots []repositorySnapshot, snapshotIDs []string) []string {
	selected := stringSet(snapshotIDs)
	runIDs := map[string]bool{}
	for _, snapshot := range snapshots {
		if !selected[snapshot.ID] {
			continue
		}
		for _, tag := range snapshot.Tags {
			if strings.HasPrefix(tag, "backupchief-run:") {
				runID := strings.TrimPrefix(tag, "backupchief-run:")
				if ulidPattern.MatchString(runID) {
					runIDs[runID] = true
				}
			}
		}
	}
	result := make([]string, 0, len(runIDs))
	for runID := range runIDs {
		result = append(result, runID)
	}
	sort.Strings(result)
	return result
}

func expandMySQLRunCandidates(snapshots []repositorySnapshot, anchors []string) []string {
	return expandDatabaseRunCandidates(snapshots, anchors)
}

func isDatabaseJob(jobType JobType) bool {
	return isMySQLJob(jobType) || isPostgreSQLJob(jobType)
}

func maintenanceRequest(job Job) restic.Request {
	return restic.Request{
		Version:    1,
		Connection: job.Repository.Connection,
		Password:   job.Repository.ServicePassword,
	}
}

func maintenanceTimeout(kind string, job Job) time.Duration {
	switch kind {
	case "forget":
		if job.Maintenance.Strategy == "after_scheduled_backup" {
			return 6 * time.Hour
		}
		return 30 * time.Minute
	case "prune":
		return 6 * time.Hour
	case "check_metadata", "snapshot_inventory":
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
		base.Diagnostic = boundedMaintenanceDiagnostic(response.Diagnostic)
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
	base.Diagnostic = boundedMaintenanceDiagnostic(response.Diagnostic)
	return base
}

func boundedMaintenanceDiagnostic(diagnostic string) string {
	diagnostic = strings.TrimSpace(diagnostic)
	if diagnostic == "" {
		return "Restic did not provide more detail."
	}

	runes := []rune(diagnostic)
	if len(runes) > 4096 {
		diagnostic = string(runes[:4096])
	}

	return diagnostic
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

func intersectsProtected(values []string, protected map[string]bool) bool {
	for _, value := range values {
		if protected[value] {
			return true
		}
	}
	return false
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
