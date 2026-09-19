package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/chieftools/backupchief-agent/restic"
)

type scriptedMaintenanceExecutor struct {
	requests []restic.Request
	results  []restic.Result
}

func TestMaintenanceRunsPrimaryThenReplicaAndRetriesTransientFailures(t *testing.T) {
	executor := &scriptedMaintenanceExecutor{results: []restic.Result{
		{ExitCode: 0, Outcome: "complete"},
		{ExitCode: 0, Outcome: "complete", Output: `{"total_size":1024}`},
		{ExitCode: 11, Outcome: "failed", Diagnostic: "synthetic repository lock"},
		{ExitCode: 0, Outcome: "complete"},
		{ExitCode: 0, Outcome: "complete", Output: `{"total_size":2048}`},
	}}
	job := maintenanceExecutionJob(t)
	job.Repository.Key = "repository_01k4p4f7m1r9d3t6v8w2x5y7ze"
	job.Replicas = []JobRepository{{
		Key: "repository_01k4p4f7m1r9d3t6v8w2x5y7zf", ID: strings.Repeat("d", 64),
		ServicePassword: "synthetic-service-password", Source: job.Repository.Key, Status: "active",
		Connection: testLocalRepository(t.TempDir()),
	}}

	result, _, _, _ := executeMaintenanceRepositories(
		context.Background(), executor, 1, maintenanceJournalCommand("prune", job.ID), job, nil,
		func(MaintenancePlan) error { return nil }, time.Now, []time.Duration{0},
	)

	if result.Status != "complete" || result.ResultCode != "success" || len(result.RepositoryResults) != 2 {
		t.Fatalf("maintenance result: %+v", result)
	}
	if result.RepositoryResults[0].AttemptCount != 1 || result.RepositoryResults[1].AttemptCount != 2 {
		t.Fatalf("repository attempts: %+v", result.RepositoryResults)
	}
	if result.RepositoryResults[0].RepositoryBytes == nil || *result.RepositoryResults[0].RepositoryBytes != 1024 || result.RepositoryResults[1].RepositoryBytes == nil || *result.RepositoryResults[1].RepositoryBytes != 2048 {
		t.Fatalf("repository sizes: %+v", result.RepositoryResults)
	}
	operations := make([]string, 0, len(executor.requests))
	for _, request := range executor.requests {
		operations = append(operations, request.Operation)
	}
	if !reflect.DeepEqual(operations, []string{"prune", "stats", "prune", "prune", "stats"}) {
		t.Fatalf("operation order: %v", operations)
	}
}

func TestRetentionResolvesProtectedSnapshotsToReplicaLocalIDs(t *testing.T) {
	latestRunID := "01k4p4f7m1r9d3t6v8w2x5y7za"
	heldRunID := "01k4p4f7m1r9d3t6v8w2x5y7zb"
	primaryLatest := strings.Repeat("a", 64)
	primaryHeld := strings.Repeat("b", 64)
	replicaLatest := strings.Repeat("c", 64)
	replicaHeld := strings.Repeat("d", 64)
	executor := &scriptedMaintenanceExecutor{results: []restic.Result{
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + primaryLatest + `","tags":["backupchief-run:` + latestRunID + `"]},{"id":"` + primaryHeld + `","tags":["backupchief-run:` + heldRunID + `"]}]`},
		{ExitCode: 0, Outcome: "complete", Output: `{"total_size":1024}`},
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + replicaLatest + `","original":"` + primaryLatest + `","tags":["backupchief-run:` + latestRunID + `"]},{"id":"` + replicaHeld + `","original":"` + primaryHeld + `","tags":["backupchief-run:` + heldRunID + `"]}]`},
		{ExitCode: 0, Outcome: "complete", Output: `{"total_size":2048}`},
	}}
	job := maintenanceExecutionJob(t)
	job.Repository.Key = "repository_01k4p4f7m1r9d3t6v8w2x5y7zc"
	job.Replicas = []JobRepository{{
		Key: "repository_01k4p4f7m1r9d3t6v8w2x5y7zd", ID: strings.Repeat("e", 64),
		ServicePassword: "synthetic-service-password", Source: job.Repository.Key, Status: "active",
		Connection: testLocalRepository(t.TempDir()),
	}}
	job.Retention.LatestComplete = &CompleteSnapshotProof{
		RunID: latestRunID, FinishedAt: "2026-09-10T08:00:00.000000Z", SnapshotIDs: []string{primaryLatest},
	}
	job.Retention.ProtectedSnapshotIDs = []string{primaryHeld}
	job.Retention.ProtectedRunIDs = []string{heldRunID}

	result, _, _, _ := executeMaintenanceRepositories(
		context.Background(), executor, 1, maintenanceJournalCommand("forget", job.ID), job, nil,
		func(MaintenancePlan) error { return nil }, time.Now, nil,
	)

	if result.Status != "complete" || result.ResultCode != "success" || len(result.RepositoryResults) != 2 {
		t.Fatalf("retention result: %+v", result)
	}
	if result.RepositoryResults[1].Status != "complete" || !reflect.DeepEqual(result.RepositoryResults[1].SnapshotEvidenceIDs, []string{replicaLatest, replicaHeld}) {
		t.Fatalf("replica result: %+v", result.RepositoryResults[1])
	}
	operations := make([]string, 0, len(executor.requests))
	for _, request := range executor.requests {
		operations = append(operations, request.Operation)
	}
	if !reflect.DeepEqual(operations, []string{"snapshots", "stats", "snapshots", "stats"}) {
		t.Fatalf("operation order: %v", operations)
	}
}

func TestRetentionFailsClosedWhenAProtectedReplicaSnapshotIsMissing(t *testing.T) {
	latestRunID := "01k4p4f7m1r9d3t6v8w2x5y7ze"
	heldRunID := "01k4p4f7m1r9d3t6v8w2x5y7zf"
	primaryLatest := strings.Repeat("1", 64)
	primaryHeld := strings.Repeat("2", 64)
	replicaLatest := strings.Repeat("3", 64)
	executor := &scriptedMaintenanceExecutor{results: []restic.Result{
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + primaryLatest + `","tags":["backupchief-run:` + latestRunID + `"]},{"id":"` + primaryHeld + `","tags":["backupchief-run:` + heldRunID + `"]}]`},
		{ExitCode: 0, Outcome: "complete", Output: `{"total_size":1024}`},
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + replicaLatest + `","original":"` + primaryLatest + `","tags":["backupchief-run:` + latestRunID + `"]}]`},
		{ExitCode: 0, Outcome: "complete", Output: `{"total_size":2048}`},
	}}
	job := maintenanceExecutionJob(t)
	job.Repository.Key = "repository_01k4p4f7m1r9d3t6v8w2x5y7zg"
	job.Replicas = []JobRepository{{
		Key: "repository_01k4p4f7m1r9d3t6v8w2x5y7zh", ID: strings.Repeat("4", 64),
		ServicePassword: "synthetic-service-password", Source: job.Repository.Key, Status: "active",
		Connection: testLocalRepository(t.TempDir()),
	}}
	job.Retention.LatestComplete = &CompleteSnapshotProof{
		RunID: latestRunID, FinishedAt: "2026-09-10T08:00:00.000000Z", SnapshotIDs: []string{primaryLatest},
	}
	job.Retention.ProtectedSnapshotIDs = []string{primaryHeld}
	job.Retention.ProtectedRunIDs = []string{heldRunID}

	result, _, _, _ := executeMaintenanceRepositories(
		context.Background(), executor, 1, maintenanceJournalCommand("forget", job.ID), job, nil,
		func(MaintenancePlan) error { return nil }, time.Now, nil,
	)

	if result.Status != "partial" || result.ResultCode != "repository_operations_incomplete" || len(result.RepositoryResults) != 2 {
		t.Fatalf("retention result: %+v", result)
	}
	if result.RepositoryResults[1].Status != "skipped" || result.RepositoryResults[1].ResultCode != "recovery_point_unavailable" {
		t.Fatalf("replica result: %+v", result.RepositoryResults[1])
	}
	for _, request := range executor.requests {
		if request.Operation == "forget" || request.Operation == "forget_plan" {
			t.Fatalf("unsafe mutation requested: %+v", request)
		}
	}
}

func (executor *scriptedMaintenanceExecutor) Run(_ context.Context, request restic.Request) restic.Result {
	executor.requests = append(executor.requests, request)
	if len(executor.results) == 0 {
		return restic.Result{ExitCode: 1, Outcome: "failed", Diagnostic: "synthetic missing response"}
	}
	result := executor.results[0]
	executor.results = executor.results[1:]
	return result
}

func TestForgetPersistsAProtectedFixedSnapshotPlanBeforeRemoval(t *testing.T) {
	first := strings.Repeat("a", 64)
	second := strings.Repeat("b", 64)
	protected := strings.Repeat("c", 64)
	executor := &scriptedMaintenanceExecutor{results: []restic.Result{
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + first + `"},{"id":"` + second + `"},{"id":"` + protected + `"}]`},
		{ExitCode: 0, Outcome: "complete", Output: `[{"remove":[{"id":"` + second + `"},{"id":"` + first + `"},{"id":"` + protected + `"}]}]`},
		{ExitCode: 0, Outcome: "complete"},
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + protected + `"}]`},
	}}
	job := maintenanceExecutionJob(t)
	job.Retention = JobRetention{
		Last: 2, Daily: 7, KeepLatestComplete: true,
		LatestComplete: &CompleteSnapshotProof{
			RunID: "01k4p4f7m1r9d3t6v8w2x5y7ze", FinishedAt: "2026-09-10T08:00:00.000000Z", SnapshotIDs: []string{protected},
		},
	}
	command := maintenanceJournalCommand("forget", job.ID)
	var persisted MaintenancePlan
	persistedBeforeForget := false
	result, _, _, _ := executeMaintenance(
		context.Background(), executor, 1, command, job, nil,
		func(plan MaintenancePlan) error {
			persisted = plan
			persistedBeforeForget = len(executor.requests) == 2
			return nil
		},
		func() time.Time { return time.Date(2026, 9, 11, 8, 0, 0, 0, time.UTC) },
	)

	if result.Status != "complete" || result.ResultCode != "success" || result.Statistics == nil {
		t.Fatalf("result: %+v", result)
	}
	if !persistedBeforeForget || !reflect.DeepEqual(persisted.CandidateSnapshotIDs, []string{first, second}) || !reflect.DeepEqual(persisted.ProtectedSnapshotIDs, []string{protected}) {
		t.Fatalf("persisted plan: %+v before-forget=%t", persisted, persistedBeforeForget)
	}
	if len(executor.requests) != 4 || executor.requests[1].Operation != "forget_plan" || executor.requests[2].Operation != "forget" || !reflect.DeepEqual(executor.requests[2].SnapshotIDs, []string{first, second}) {
		t.Fatalf("requests: %+v", executor.requests)
	}
	wantStatistics := RunStatistics{
		"snapshots_considered": uint64(3), "snapshots_removed": uint64(2),
		"snapshots_retained": uint64(1), "snapshots_protected": uint64(1),
	}
	if !reflect.DeepEqual(*result.Statistics, wantStatistics) {
		t.Fatalf("statistics: %+v", *result.Statistics)
	}
	if result.SnapshotEvidence == nil || result.SnapshotEvidence.Scope != "repository" || !reflect.DeepEqual(result.SnapshotEvidenceIDs, []string{protected}) {
		t.Fatalf("snapshot evidence: %+v %v", result.SnapshotEvidence, result.SnapshotEvidenceIDs)
	}
}

func TestPolicyForgetPreservesForeverProtectedSnapshots(t *testing.T) {
	remove := strings.Repeat("d", 64)
	held := strings.Repeat("e", 64)
	latest := strings.Repeat("f", 64)
	executor := &scriptedMaintenanceExecutor{results: []restic.Result{
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + remove + `"},{"id":"` + held + `"},{"id":"` + latest + `"}]`},
		{ExitCode: 0, Outcome: "complete", Output: `[{"remove":[{"id":"` + remove + `"},{"id":"` + held + `"},{"id":"` + latest + `"}]}]`},
		{ExitCode: 0, Outcome: "complete"},
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + held + `"},{"id":"` + latest + `"}]`},
	}}
	job := maintenanceExecutionJob(t)
	job.Retention.Daily = 7
	job.Retention.LatestComplete = &CompleteSnapshotProof{
		RunID: "01k4p4f7m1r9d3t6v8w2x5y7ze", FinishedAt: "2026-09-10T08:00:00.000000Z", SnapshotIDs: []string{latest},
	}
	job.Retention.ProtectedSnapshotIDs = []string{held}
	var persisted MaintenancePlan

	result, _, _, _ := executeMaintenance(
		context.Background(), executor, 1, maintenanceJournalCommand("forget", job.ID), job, nil,
		func(plan MaintenancePlan) error { persisted = plan; return nil }, time.Now,
	)

	if result.Status != "complete" || result.ResultCode != "success" {
		t.Fatalf("policy result: %+v", result)
	}
	if !reflect.DeepEqual(persisted.CandidateSnapshotIDs, []string{remove}) || !reflect.DeepEqual(persisted.ProtectedSnapshotIDs, []string{held, latest}) {
		t.Fatalf("policy plan: %+v", persisted)
	}
	if len(executor.requests) != 4 || !reflect.DeepEqual(executor.requests[2].SnapshotIDs, []string{remove}) {
		t.Fatalf("policy requests: %+v", executor.requests)
	}
}

func TestTargetedForgetExpiresTheWholeDatabaseRunWithoutPolicyPlanning(t *testing.T) {
	anchor := strings.Repeat("1", 64)
	peer := strings.Repeat("2", 64)
	latest := strings.Repeat("3", 64)
	held := strings.Repeat("4", 64)
	executor := &scriptedMaintenanceExecutor{results: []restic.Result{
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + anchor + `","tags":["backupchief-run:synthetic-run","backupchief-run-anchor"]},{"id":"` + peer + `","tags":["backupchief-run:synthetic-run"]},{"id":"` + latest + `"},{"id":"` + held + `"}]`},
		{ExitCode: 0, Outcome: "complete"},
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + latest + `"},{"id":"` + held + `"}]`},
	}}
	job := maintenanceExecutionJob(t)
	job.Type = JobTypeMySQL
	job.Retention.LatestComplete = &CompleteSnapshotProof{
		RunID: "01k4p4f7m1r9d3t6v8w2x5y7ze", FinishedAt: "2026-09-10T08:00:00.000000Z", SnapshotIDs: []string{latest},
	}
	job.Retention.ProtectedSnapshotIDs = []string{held}
	command := maintenanceJournalCommand("forget", job.ID)
	command.Command.Payload.SnapshotIDs = []string{anchor}
	var persisted MaintenancePlan
	persistedBeforeForget := false

	result, _, _, _ := executeMaintenance(
		context.Background(), executor, 1, command, job, nil,
		func(plan MaintenancePlan) error {
			persisted = plan
			persistedBeforeForget = len(executor.requests) == 1
			return nil
		},
		time.Now,
	)

	if result.Status != "complete" || result.ResultCode != "success" || !persistedBeforeForget {
		t.Fatalf("targeted result=%+v persisted-before-forget=%t", result, persistedBeforeForget)
	}
	if !reflect.DeepEqual(persisted.CandidateSnapshotIDs, []string{anchor, peer}) || !reflect.DeepEqual(persisted.ProtectedSnapshotIDs, []string{latest, held}) {
		t.Fatalf("targeted plan: %+v", persisted)
	}
	if len(executor.requests) != 3 || executor.requests[1].Operation != "forget" || !reflect.DeepEqual(executor.requests[1].SnapshotIDs, []string{anchor, peer}) {
		t.Fatalf("targeted requests: %+v", executor.requests)
	}
}

func TestTargetedForgetRefusesAProtectedSnapshotAsOneAction(t *testing.T) {
	target := strings.Repeat("5", 64)
	latest := strings.Repeat("6", 64)
	executor := &scriptedMaintenanceExecutor{results: []restic.Result{{
		ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + target + `"},{"id":"` + latest + `"}]`,
	}}}
	job := maintenanceExecutionJob(t)
	job.Retention.LatestComplete = &CompleteSnapshotProof{
		RunID: "01k4p4f7m1r9d3t6v8w2x5y7ze", FinishedAt: "2026-09-10T08:00:00.000000Z", SnapshotIDs: []string{latest},
	}
	job.Retention.ProtectedSnapshotIDs = []string{target}
	command := maintenanceJournalCommand("forget", job.ID)
	command.Command.Payload.SnapshotIDs = []string{target}

	result, _, _, _ := executeMaintenance(
		context.Background(), executor, 1, command, job, nil,
		func(MaintenancePlan) error { t.Fatal("protected target was persisted"); return nil }, time.Now,
	)

	if result.Status != "skipped" || result.ResultCode != "recovery_point_protected" || len(executor.requests) != 1 || result.SnapshotEvidence == nil || result.SnapshotEvidence.Scope != "repository" {
		t.Fatalf("protected target result=%+v requests=%+v", result, executor.requests)
	}
}

func TestTargetedForgetTreatsAnAbsentSnapshotAsAlreadyExpired(t *testing.T) {
	absent := strings.Repeat("7", 64)
	latest := strings.Repeat("8", 64)
	executor := &scriptedMaintenanceExecutor{results: []restic.Result{{
		ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + latest + `"}]`,
	}}}
	job := maintenanceExecutionJob(t)
	job.Retention.LatestComplete = &CompleteSnapshotProof{
		RunID: "01k4p4f7m1r9d3t6v8w2x5y7ze", FinishedAt: "2026-09-10T08:00:00.000000Z", SnapshotIDs: []string{latest},
	}
	command := maintenanceJournalCommand("forget", job.ID)
	command.Command.Payload.SnapshotIDs = []string{absent}

	result, _, _, _ := executeMaintenance(
		context.Background(), executor, 1, command, job, nil, func(MaintenancePlan) error { return nil }, time.Now,
	)

	if result.Status != "complete" || result.ResultCode != "success" || len(executor.requests) != 1 || result.SnapshotEvidence == nil {
		t.Fatalf("absent target result=%+v requests=%+v", result, executor.requests)
	}
}

func TestCombinedRetentionRunsPruneWhenDueAndRecordsItsCompletion(t *testing.T) {
	remove := strings.Repeat("8", 64)
	protected := strings.Repeat("9", 64)
	executor := &scriptedMaintenanceExecutor{results: []restic.Result{
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + remove + `"},{"id":"` + protected + `"}]`},
		{ExitCode: 0, Outcome: "complete", Output: `[{"remove":[{"id":"` + remove + `"}]}]`},
		{ExitCode: 0, Outcome: "complete"},
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + protected + `"}]`},
		{ExitCode: 0, Outcome: "complete"},
	}}
	job := maintenanceExecutionJob(t)
	job.Maintenance = JobMaintenance{Strategy: "after_scheduled_backup", MaxDeferralSeconds: 86400, PruneIntervalSeconds: 604800}
	job.Retention.Daily = 1
	job.Retention.LatestComplete = &CompleteSnapshotProof{
		RunID: "01k4p4f7m1r9d3t6v8w2x5y7zd", FinishedAt: "2026-09-10T08:00:00.000000Z", SnapshotIDs: []string{protected},
	}
	plan := MaintenancePlan{Kind: "forget", CombinedRetention: true, PruneAfterForget: true}
	persisted := plan

	result, _, _, _ := executeMaintenance(
		context.Background(), executor, 1, maintenanceJournalCommand("forget", job.ID), job, &plan,
		func(updated MaintenancePlan) error { persisted = updated; return nil },
		func() time.Time { return time.Date(2026, 9, 11, 8, 0, 0, 0, time.UTC) },
	)

	if result.Status != "complete" || result.ResultCode != "success" || !result.PruneCompleted || result.Statistics == nil {
		t.Fatalf("combined retention result: %+v", result)
	}
	if (*result.Statistics)["prune_status"] != "complete" || !persisted.ForgetPlanned || !persisted.PruneStarted || !persisted.PruneCompleted {
		t.Fatalf("combined retention state: statistics=%+v plan=%+v", *result.Statistics, persisted)
	}
	if len(executor.requests) != 5 || executor.requests[4].Operation != "prune" {
		t.Fatalf("combined retention requests: %+v", executor.requests)
	}
}

func TestCombinedRetentionLeavesPruneDueAfterAPhaseFailure(t *testing.T) {
	protected := strings.Repeat("7", 64)
	executor := &scriptedMaintenanceExecutor{results: []restic.Result{
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + protected + `"}]`},
		{ExitCode: 0, Outcome: "complete", Output: `[]`},
		{ExitCode: 11, Outcome: "locked"},
		{ExitCode: 11, Outcome: "locked"},
	}}
	job := maintenanceExecutionJob(t)
	job.Maintenance = JobMaintenance{Strategy: "after_scheduled_backup", MaxDeferralSeconds: 86400, PruneIntervalSeconds: 604800}
	job.Retention.Daily = 1
	job.Retention.LatestComplete = &CompleteSnapshotProof{
		RunID: "01k4p4f7m1r9d3t6v8w2x5y7ze", FinishedAt: "2026-09-10T08:00:00.000000Z", SnapshotIDs: []string{protected},
	}
	plan := MaintenancePlan{Kind: "forget", CombinedRetention: true, PruneAfterForget: true}

	result, _, _, _ := executeMaintenance(
		context.Background(), executor, 1, maintenanceJournalCommand("forget", job.ID), job, &plan,
		func(MaintenancePlan) error { return nil }, time.Now,
	)

	if result.Status != "failed" || result.ResultCode != "repository_locked" || result.PruneCompleted || result.Statistics == nil || (*result.Statistics)["prune_status"] != "failed" {
		t.Fatalf("failed prune phase result: %+v", result)
	}
}

func TestSnapshotInventoryReportsTheExactRepositoryState(t *testing.T) {
	first := strings.Repeat("4", 64)
	second := strings.Repeat("5", 64)
	executor := &scriptedMaintenanceExecutor{results: []restic.Result{{
		ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + second + `"},{"id":"` + first + `"}]`,
	}}}
	job := maintenanceExecutionJob(t)
	result, _, _, _ := executeMaintenance(
		context.Background(), executor, 1, maintenanceJournalCommand("snapshot_inventory", job.ID), job,
		nil, func(MaintenancePlan) error { return nil },
		func() time.Time { return time.Date(2026, 9, 12, 9, 0, 0, 0, time.UTC) },
	)

	if result.Status != "complete" || result.ResultCode != "success" || result.SnapshotEvidence == nil {
		t.Fatalf("inventory result: %+v", result)
	}
	if result.SnapshotEvidence.Scope != "repository" || result.SnapshotEvidence.SnapshotCount != 2 || result.SnapshotEvidence.ChunkCount != 1 {
		t.Fatalf("inventory manifest: %+v", result.SnapshotEvidence)
	}
	if !reflect.DeepEqual(result.SnapshotEvidenceIDs, []string{first, second}) || executor.requests[0].Operation != "snapshots" || !executor.requests[0].RecoverStaleLocks {
		t.Fatalf("inventory ids=%v requests=%+v", result.SnapshotEvidenceIDs, executor.requests)
	}
}

func TestForgetResumesThePersistedFixedPlanWithoutRecomputingPolicy(t *testing.T) {
	remove := strings.Repeat("d", 64)
	protected := strings.Repeat("e", 64)
	executor := &scriptedMaintenanceExecutor{results: []restic.Result{
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + remove + `"},{"id":"` + protected + `"}]`},
		{ExitCode: 0, Outcome: "complete"},
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + protected + `"}]`},
	}}
	job := maintenanceExecutionJob(t)
	job.Retention.LatestComplete = &CompleteSnapshotProof{
		RunID: "01k4p4f7m1r9d3t6v8w2x5y7zf", FinishedAt: "2026-09-10T09:00:00.000000Z", SnapshotIDs: []string{protected},
	}
	plan := MaintenancePlan{Kind: "forget", CandidateSnapshotIDs: []string{remove}, ProtectedSnapshotIDs: []string{protected}}
	result, _, _, _ := executeMaintenance(
		context.Background(), executor, 1, maintenanceJournalCommand("forget", job.ID), job, &plan,
		func(MaintenancePlan) error { t.Fatal("a resumed plan was persisted again"); return nil }, time.Now,
	)

	if result.ResultCode != "success" || len(executor.requests) != 3 || executor.requests[1].Operation != "forget" {
		t.Fatalf("resumed result=%+v requests=%+v", result, executor.requests)
	}
}

func TestForgetRetriesOnceWithStaleLockRecoveryWhenARepositoryLockAppearsAfterPreflight(t *testing.T) {
	remove := strings.Repeat("6", 64)
	protected := strings.Repeat("7", 64)
	executor := &scriptedMaintenanceExecutor{results: []restic.Result{
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + remove + `"},{"id":"` + protected + `"}]`},
		{ExitCode: 0, Outcome: "complete", Output: `[{"remove":[{"id":"` + remove + `"}]}]`},
		{ExitCode: 11, Outcome: "locked"},
		{ExitCode: 0, Outcome: "complete"},
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + protected + `"}]`},
	}}
	job := maintenanceExecutionJob(t)
	job.Retention = JobRetention{
		Daily: 2, KeepLatestComplete: true,
		LatestComplete: &CompleteSnapshotProof{
			RunID: "01k4p4f7m1r9d3t6v8w2x5y7zh", FinishedAt: "2026-09-10T10:00:00.000000Z", SnapshotIDs: []string{protected},
		},
	}
	result, _, _, _ := executeMaintenance(
		context.Background(), executor, 1, maintenanceJournalCommand("forget", job.ID), job, nil,
		func(MaintenancePlan) error { return nil }, time.Now,
	)

	if result.ResultCode != "success" || len(executor.requests) != 5 {
		t.Fatalf("result=%+v requests=%+v", result, executor.requests)
	}
	if !executor.requests[0].RecoverStaleLocks || executor.requests[2].RecoverStaleLocks || !executor.requests[3].RecoverStaleLocks {
		t.Fatalf("stale lock recovery requests: %+v", executor.requests)
	}
}

func TestMaintenanceRejectsPersistedPlansWithoutTheExpectedKind(t *testing.T) {
	job := maintenanceExecutionJob(t)
	for _, test := range []struct {
		name string
		plan MaintenancePlan
	}{
		{name: "missing kind", plan: MaintenancePlan{}},
		{name: "different kind", plan: MaintenancePlan{Kind: "prune"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor := &scriptedMaintenanceExecutor{}
			result, _, _, _ := executeMaintenance(
				context.Background(), executor, 1, maintenanceJournalCommand("forget", job.ID), job, &test.plan,
				func(MaintenancePlan) error { t.Fatal("an invalid plan was persisted"); return nil }, time.Now,
			)

			if result.Status != "failed" || result.ResultCode != "execution_failed" || len(executor.requests) != 0 {
				t.Fatalf("result=%+v requests=%+v", result, executor.requests)
			}
		})
	}
}

func TestRetentionStopsBeforeMutationWithoutCentralSafetyProof(t *testing.T) {
	protected := strings.Repeat("f", 64)
	for _, test := range []struct {
		name      string
		retention JobRetention
		results   []restic.Result
		wantCode  string
	}{
		{name: "unresolved run", retention: JobRetention{HasUnresolvedRuns: true}, wantCode: "unresolved_runs_present"},
		{name: "no complete proof", retention: JobRetention{}, wantCode: "recovery_point_unavailable"},
		{
			name: "protected snapshot missing",
			retention: JobRetention{LatestComplete: &CompleteSnapshotProof{
				RunID: "01k4p4f7m1r9d3t6v8w2x5y7zg", FinishedAt: "2026-09-10T10:00:00.000000Z", SnapshotIDs: []string{protected},
			}},
			results:  []restic.Result{{ExitCode: 0, Outcome: "complete", Output: `[]`}},
			wantCode: "recovery_point_unavailable",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor := &scriptedMaintenanceExecutor{results: test.results}
			job := maintenanceExecutionJob(t)
			job.Retention = test.retention
			result, _, _, _ := executeMaintenance(
				context.Background(), executor, 1, maintenanceJournalCommand("forget", job.ID), job, nil,
				func(MaintenancePlan) error { t.Fatal("unsafe plan was persisted"); return nil }, time.Now,
			)
			if result.Status != "skipped" || result.ResultCode != test.wantCode {
				t.Fatalf("result: %+v", result)
			}
			for _, request := range executor.requests {
				if request.Operation == "forget" || request.Operation == "forget_plan" {
					t.Fatalf("unsafe mutation requested: %+v", request)
				}
			}
		})
	}
}

func TestChecksReportDepthRotationAndRepositoryFailures(t *testing.T) {
	job := maintenanceExecutionJob(t)
	metadataExecutor := &scriptedMaintenanceExecutor{results: []restic.Result{{
		ExitCode: 0, Outcome: "complete", Output: "{\"message_type\":\"summary\",\"num_errors\":0}\n",
	}}}
	metadata, _, _, _ := executeMaintenance(
		context.Background(), metadataExecutor, 1, maintenanceJournalCommand("check_metadata", job.ID), job,
		nil, func(MaintenancePlan) error { return nil }, time.Now,
	)
	if metadata.ResultCode != "success" || metadata.Statistics == nil || (*metadata.Statistics)["data_checked"] != false || metadataExecutor.requests[0].Operation != "check_metadata" {
		t.Fatalf("metadata check: %+v requests=%+v", metadata, metadataExecutor.requests)
	}

	dataExecutor := &scriptedMaintenanceExecutor{results: []restic.Result{{
		ExitCode: 1, Outcome: "failed", Output: "{\"message_type\":\"summary\",\"num_errors\":2}\n",
	}}}
	data, _, _, _ := executeMaintenance(
		context.Background(), dataExecutor, 1, maintenanceJournalCommand("check_data", job.ID), job,
		&MaintenancePlan{Kind: "check_data", DataSubsetPart: 3, DataSubsetTotal: 7}, func(MaintenancePlan) error { return nil }, time.Now,
	)
	if data.Status != "failed" || data.ResultCode != "repository_corrupt" || data.Statistics == nil || (*data.Statistics)["errors_found"] != uint64(2) {
		t.Fatalf("data check: %+v", data)
	}
	if dataExecutor.requests[0].DataSubsetPart != 3 || dataExecutor.requests[0].DataSubsetTotal != 7 {
		t.Fatalf("data subset: %+v", dataExecutor.requests[0])
	}

	for exitCode, want := range map[int]string{10: "repository_missing", 11: "repository_locked", 12: "repository_authentication_failed"} {
		executor := &scriptedMaintenanceExecutor{results: []restic.Result{{ExitCode: exitCode, Outcome: "failed"}}}
		result, _, _, _ := executeMaintenance(
			context.Background(), executor, 1, maintenanceJournalCommand("prune", job.ID), job,
			nil, func(MaintenancePlan) error { return nil }, time.Now,
		)
		if result.ResultCode != want {
			t.Fatalf("exit %d: %+v", exitCode, result)
		}
	}
}

func TestMaintenanceRuntimeClearsOnlyAfterCentralConfirmationAndRotatesSuccessfulDataChecks(t *testing.T) {
	store := newAgentTestStore(t)
	job := maintenanceExecutionJob(t)
	job.Integrity.DataParts = 4
	config := Config{Revision: 2, Jobs: []Job{job}}
	runtime := &daemon{
		store: store, metadata: ConfigMetadata{Revision: 2},
		state: RuntimeState{Maintenance: map[string]MaintenanceRuntime{
			job.Repository.ID: {Unresolved: true, UnresolvedAtRevision: 2, DataParts: 4, NextDataPart: 1},
		}},
		journal: newCommandJournal(), active: map[string]context.CancelFunc{},
	}
	runtime.acceptMaintenanceConfigLocked(config)
	if !runtime.state.Maintenance[job.Repository.ID].Unresolved {
		t.Fatal("the same central revision cleared a local unresolved marker")
	}
	config.Revision = 3
	runtime.acceptMaintenanceConfigLocked(config)
	if !runtime.state.Maintenance[job.Repository.ID].Unresolved {
		t.Fatal("a newer config cleared an unresolved result before central confirmation")
	}
	config.Jobs[0].Retention.HasUnresolvedRuns = true
	runtime.acceptMaintenanceConfigLocked(config)
	config.Revision = 4
	config.Jobs[0].Retention.HasUnresolvedRuns = false
	runtime.acceptMaintenanceConfigLocked(config)
	if runtime.state.Maintenance[job.Repository.ID].Unresolved {
		t.Fatal("a later safe central revision did not clear the confirmed unresolved marker")
	}
	runtime.recordOutcomeLocked(job, CommandResult{RunKind: "check_data", Status: "complete", ResultCode: "success"})
	if runtime.state.Maintenance[job.Repository.ID].NextDataPart != 2 {
		t.Fatalf("data rotation: %+v", runtime.state.Maintenance[job.Repository.ID])
	}
	runtime.state.Maintenance[job.Repository.ID] = MaintenanceRuntime{Unresolved: true}
	runtime.recordOutcomeLocked(job, CommandResult{
		RunKind: "snapshot_inventory", Status: "complete", ResultCode: "success",
		SnapshotEvidence: &SnapshotEvidence{Scope: "repository"},
	})
	if runtime.state.Maintenance[job.Repository.ID].Unresolved {
		t.Fatal("a verified full inventory did not clear local repository uncertainty")
	}
}

func TestCombinedRetentionPersistsTheWeeklyPruneCadence(t *testing.T) {
	store := newAgentTestStore(t)
	job := maintenanceExecutionJob(t)
	job.Maintenance = JobMaintenance{Strategy: "after_scheduled_backup", MaxDeferralSeconds: 86400, PruneIntervalSeconds: 604800}
	now := time.Date(2026, 9, 14, 3, 0, 0, 0, time.UTC)
	runtime := &daemon{
		store: store,
		now:   func() time.Time { return now },
		state: RuntimeState{Maintenance: map[string]MaintenanceRuntime{}},
	}

	_, firstPlan := runtime.prepareMaintenanceLocked(job, maintenanceJournalCommand("forget", job.ID))
	if firstPlan == nil || !firstPlan.CombinedRetention || !firstPlan.PruneAfterForget {
		t.Fatalf("initial combined retention plan: %+v", firstPlan)
	}
	runtime.recordOutcomeLocked(job, CommandResult{
		RunKind: "forget", Status: "complete", ResultCode: "success", FinishedAt: protocolTimestamp(now), PruneCompleted: true,
	})

	now = now.Add(6 * 24 * time.Hour)
	_, beforeInterval := runtime.prepareMaintenanceLocked(job, maintenanceJournalCommand("forget", job.ID))
	if beforeInterval == nil || beforeInterval.PruneAfterForget {
		t.Fatalf("prune became due before its interval: %+v", beforeInterval)
	}

	now = now.Add(24 * time.Hour)
	_, atInterval := runtime.prepareMaintenanceLocked(job, maintenanceJournalCommand("forget", job.ID))
	if atInterval == nil || !atInterval.PruneAfterForget {
		t.Fatalf("prune was not due at its interval: %+v", atInterval)
	}
}

func TestDisablingAJobCancelsItsQueuedCatchUpBackup(t *testing.T) {
	job := maintenanceExecutionJob(t)
	commandID := "01k4p4f7m1r9d3t6v8w2x5y7za"
	runtime := &daemon{
		state: RuntimeState{Maintenance: map[string]MaintenanceRuntime{}},
		journal: CommandJournal{Commands: map[string]*JournalCommand{
			commandID: {
				Command: AgentCommand{Payload: CommandPayload{JobID: job.ID}},
				RunKind: "backup", State: "received", CatchUpBackup: true,
			},
		}},
	}

	_, queued := runtime.acceptMaintenanceConfigLocked(Config{Jobs: []Job{}})

	if !reflect.DeepEqual(queued, []string{commandID}) {
		t.Fatalf("queued cancellations: %v", queued)
	}
}

func TestInterruptedMaintenanceRelistsFixedForgetPlansAndLeavesOtherOutcomesUnresolved(t *testing.T) {
	first := strings.Repeat("1", 64)
	second := strings.Repeat("2", 64)
	protected := strings.Repeat("3", 64)
	job := maintenanceExecutionJob(t)
	executor := &scriptedMaintenanceExecutor{results: []restic.Result{{
		ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + second + `"},{"id":"` + protected + `"}]`,
	}}}
	runtime := &daemon{bootstrap: Bootstrap{Generation: 1}, executor: executor, now: time.Now}
	command := maintenanceJournalCommand("forget", job.ID)
	command.ReceivedAt = "2026-09-11T08:00:00.000000Z"
	command.MaintenancePlan = &MaintenancePlan{
		Kind: "forget", CandidateSnapshotIDs: []string{first, second}, ProtectedSnapshotIDs: []string{protected},
	}

	result := runtime.reconciledMaintenance(context.Background(), command, job)
	if result.Status != "partial" || result.ResultCode != "snapshot_removal_incomplete" || result.Statistics == nil || (*result.Statistics)["snapshots_removed"] != uint64(1) {
		t.Fatalf("reconciled forget: %+v", result)
	}
	command.RunKind = "prune"
	command.MaintenancePlan = &MaintenancePlan{Kind: "prune"}
	result = runtime.reconciledMaintenance(context.Background(), command, job)
	if result.Status != "unresolved" || result.ResultCode != "outcome_unresolved" {
		t.Fatalf("reconciled prune: %+v", result)
	}
}

func TestScheduledMaintenanceCompletesOfflineAndReplaysAfterRestart(t *testing.T) {
	store := newAgentTestStore(t)
	candidate := strings.Repeat("5", 64)
	protected := strings.Repeat("4", 64)
	job := maintenanceExecutionJob(t)
	job.Schedule = JobSchedule{Kind: "cron", Expression: "0 2 * * *", Timezone: "UTC"}
	job.Retention.ForgetCron = "0 1 * * *"
	job.Retention.LatestComplete = &CompleteSnapshotProof{
		RunID: "01k4p4f7m1r9d3t6v8w2x5y7zh", FinishedAt: "2026-09-10T01:00:00.000000Z", SnapshotIDs: []string{protected},
	}
	job.Retention.Hourly = 4
	executor := &scriptedMaintenanceExecutor{results: []restic.Result{
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + candidate + `"},{"id":"` + protected + `"}]`},
		{ExitCode: 0, Outcome: "complete", Output: `[{"remove":[{"id":"` + candidate + `"}]}]`},
		{ExitCode: 0, Outcome: "complete"},
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + protected + `"}]`},
	}}
	config := schedulerConfig(job)
	now := time.Date(2026, 9, 11, 0, 59, 0, 0, time.UTC)
	runtime := newSchedulerDaemon(t, store, config, &now, executor)
	now = time.Date(2026, 9, 11, 1, 0, 0, 0, time.UTC)
	if err := runtime.scheduleBackups(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		runtime.mu.Lock()
		finished := len(runtime.journal.Commands) == 1
		for _, command := range runtime.journal.Commands {
			finished = finished && command.State == "finished" && command.RunKind == "forget"
		}
		runtime.mu.Unlock()
		if finished {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if len(executor.requests) != 5 || executor.requests[0].Operation != "snapshots" || executor.requests[1].Operation != "forget_plan" || executor.requests[2].Operation != "forget" || executor.requests[3].Operation != "snapshots" || executor.requests[4].Operation != "stats" {
		t.Fatalf("scheduled retention requests: %+v", executor.requests)
	}

	received := make([]AgentEvent, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		switch {
		case request.URL.Path == "/agent/v1/events":
			var envelope EventRequest
			if err := json.NewDecoder(request.Body).Decode(&envelope); err != nil {
				t.Errorf("events: %v", err)
			}
			received = append(received, envelope.Events...)
			results := make([]EventResult, 0, len(envelope.Events))
			for _, event := range envelope.Events {
				results = append(results, EventResult{ID: event.ID, Status: "accepted"})
			}
			_ = json.NewEncoder(response).Encode(EventsResponse{ProtocolRevision: ProtocolRevision, Results: results})
		case strings.Contains(request.URL.Path, "/logs/") && strings.Contains(request.URL.Path, "/chunks/"):
			_, _ = io.Copy(io.Discard, request.Body)
			response.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(request.URL.Path, "/complete"):
			response.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(request.URL.Path, "/result"):
			t.Error("scheduled maintenance submitted a command result")
			response.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()
	restarted := newSchedulerDaemon(t, store, config, &now, executor)
	restarted.client = NewClient(server.URL+"/agent/v1", testBootstrap().Credential, "1.1.0", server.Client())
	if err := restarted.reportJournal(context.Background()); err != nil {
		t.Fatal(err)
	}
	journal, err := store.LoadCommandJournal()
	if err != nil || len(journal.Commands) != 0 {
		t.Fatalf("replayed journal: %+v %v", journal, err)
	}
	if len(received) != 3 || received[0].RunKind != "forget" || received[1].Kind != "snapshot_inventory_chunk" || received[2].Kind != "run_finished" {
		t.Fatalf("replayed events: %+v", received)
	}
}

func TestSchedulerEnforcesTwoBackupOneMaintenanceAndRepositorySerialization(t *testing.T) {
	store := newAgentTestStore(t)
	release := make(chan struct{})
	executor := &schedulerExecutor{release: release}
	now := time.Date(2026, 9, 13, 0, 59, 0, 0, time.UTC)
	jobs := make([]Job, 4)
	for index := range jobs {
		jobs[index] = maintenanceExecutionJob(t)
		jobs[index].ID = []string{
			"01k4p4f7m1r9d3t6v8w2x5y7za", "01k4p4f7m1r9d3t6v8w2x5y7zb",
			"01k4p4f7m1r9d3t6v8w2x5y7zc", "01k4p4f7m1r9d3t6v8w2x5y7zd",
		}[index]
		jobs[index].Repository.ID = strings.Repeat(string(rune('a'+index)), 64)
		jobs[index].Schedule = JobSchedule{Kind: "cron", Expression: "0 2 * * *", Timezone: "UTC"}
		jobs[index].Retention.ForgetCron = "0 1 * * *"
		jobs[index].Retention.LatestComplete = &CompleteSnapshotProof{
			RunID: "01k4p4f7m1r9d3t6v8w2x5y7ze", FinishedAt: "2026-09-12T01:00:00.000000Z", SnapshotIDs: []string{strings.Repeat("f", 64)},
		}
	}
	jobs[0].Schedule.Expression = "0 1 * * *"
	jobs[1].Schedule.Expression = "0 1 * * *"
	config := Config{ProtocolRevision: ProtocolRevision, Generation: 1, Revision: 2, SchemaVersion: ConfigSchemaVersion, Jobs: jobs}
	runtime := newSchedulerDaemon(t, store, config, &now, executor)
	now = time.Date(2026, 9, 13, 1, 0, 0, 0, time.UTC)
	if err := runtime.scheduleBackups(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForScheduledExecutions(t, executor, 3)

	runtime.mu.Lock()
	overlaps := 0
	for _, command := range runtime.journal.Commands {
		if command.Result != nil && command.Result.ResultCode == "schedule_overlap" {
			overlaps++
		}
	}
	runtime.mu.Unlock()
	if overlaps != 3 {
		t.Fatalf("overlaps=%d journal=%d", overlaps, len(runtime.journal.Commands))
	}
	close(release)
	waitForAllScheduledRuns(t, runtime, 6)
}

func maintenanceExecutionJob(t *testing.T) Job {
	t.Helper()
	job := executionJob(t.TempDir())
	job.Repository.ID = strings.Repeat("9", 64)
	job.Retention = JobRetention{KeepLatestComplete: true, ForgetCron: "17 3 * * *", PruneCron: "47 15 * * 0"}
	job.Integrity = JobIntegrity{MetadataCron: "0 2 * * 0", DataMode: "auto", DataCron: "0 3 * * 0", DataParts: 4}
	return job
}

func maintenanceJournalCommand(kind, jobID string) *JournalCommand {
	return &JournalCommand{
		RunID: "01k4p4f7m1r9d3t6v8w2x5y7zc", RunKind: kind, ConfigRevision: 2, Trigger: "scheduled",
		Command: AgentCommand{Payload: CommandPayload{JobID: jobID, RequiredConfigRevision: 2, Maintenance: kind}},
	}
}

func TestExpandMySQLRunCandidatesKeepsRunSnapshotsTogether(t *testing.T) {
	snapshots := []repositorySnapshot{
		{ID: strings.Repeat("a", 64), Tags: []string{"backupchief-run:run-one", "backupchief-run-anchor"}},
		{ID: strings.Repeat("b", 64), Tags: []string{"backupchief-run:run-one"}},
		{ID: strings.Repeat("c", 64), Tags: []string{"backupchief-run:run-two", "backupchief-run-anchor"}},
		{ID: strings.Repeat("d", 64), Tags: []string{"backupchief-run:run-two"}},
	}

	result := expandMySQLRunCandidates(snapshots, []string{strings.Repeat("c", 64)})

	if len(result) != 2 || result[0] != strings.Repeat("c", 64) || result[1] != strings.Repeat("d", 64) {
		t.Fatalf("expanded snapshots: %#v", result)
	}
}
