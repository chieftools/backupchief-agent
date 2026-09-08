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
		context.Background(), executor, 1, command, job, MaintenancePlan{},
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
		context.Background(), executor, 1, maintenanceJournalCommand("forget", job.ID), job, plan,
		func(MaintenancePlan) error { t.Fatal("a resumed plan was persisted again"); return nil }, time.Now,
	)

	if result.ResultCode != "success" || len(executor.requests) != 3 || executor.requests[1].Operation != "forget" {
		t.Fatalf("resumed result=%+v requests=%+v", result, executor.requests)
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
				context.Background(), executor, 1, maintenanceJournalCommand("forget", job.ID), job, MaintenancePlan{},
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
		MaintenancePlan{Kind: "check_metadata"}, func(MaintenancePlan) error { return nil }, time.Now,
	)
	if metadata.ResultCode != "success" || metadata.Statistics == nil || (*metadata.Statistics)["data_checked"] != false || metadataExecutor.requests[0].Operation != "check_metadata" {
		t.Fatalf("metadata check: %+v requests=%+v", metadata, metadataExecutor.requests)
	}

	dataExecutor := &scriptedMaintenanceExecutor{results: []restic.Result{{
		ExitCode: 1, Outcome: "failed", Output: "{\"message_type\":\"summary\",\"num_errors\":2}\n",
	}}}
	data, _, _, _ := executeMaintenance(
		context.Background(), dataExecutor, 1, maintenanceJournalCommand("check_data", job.ID), job,
		MaintenancePlan{Kind: "check_data", DataSubsetPart: 3, DataSubsetTotal: 7}, func(MaintenancePlan) error { return nil }, time.Now,
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
			MaintenancePlan{Kind: "prune"}, func(MaintenancePlan) error { return nil }, time.Now,
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
	protected := strings.Repeat("4", 64)
	job := maintenanceExecutionJob(t)
	job.Schedule = JobSchedule{Kind: "cron", Expression: "0 2 * * *", Timezone: "UTC"}
	job.Retention.ForgetCron = "0 1 * * *"
	job.Retention.LatestComplete = &CompleteSnapshotProof{
		RunID: "01k4p4f7m1r9d3t6v8w2x5y7zh", FinishedAt: "2026-09-10T01:00:00.000000Z", SnapshotIDs: []string{protected},
	}
	executor := &scriptedMaintenanceExecutor{results: []restic.Result{{
		ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + protected + `"}]`,
	}}}
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
	restarted.client = NewClient(server.URL+"/agent/v1", testBootstrap().Credential, "1.0.0", server.Client())
	if err := restarted.reportJournal(context.Background()); err != nil {
		t.Fatal(err)
	}
	journal, err := store.LoadCommandJournal()
	if err != nil || len(journal.Commands) != 0 {
		t.Fatalf("replayed journal: %+v %v", journal, err)
	}
	if len(received) != 2 || received[0].RunKind != "forget" || received[1].RunKind != "forget" || received[1].Kind != "run_finished" {
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
