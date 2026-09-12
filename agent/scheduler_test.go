package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chieftools/backupchief-agent/restic"
	bolt "go.etcd.io/bbolt"
)

type schedulerExecutor struct {
	mu       sync.Mutex
	requests []restic.Request
	release  <-chan struct{}
}

func (executor *schedulerExecutor) Run(ctx context.Context, request restic.Request) restic.Result {
	executor.mu.Lock()
	executor.requests = append(executor.requests, request)
	executor.mu.Unlock()
	if request.Operation == "stats" {
		return restic.Result{ExitCode: 0, Outcome: "complete", Output: `{"total_size":4096}`}
	}
	if executor.release != nil {
		select {
		case <-executor.release:
		case <-ctx.Done():
			return restic.Result{ExitCode: 1, Outcome: "cancelled", Diagnostic: "synthetic scheduler cancellation"}
		}
	}
	return restic.Result{
		ExitCode: 0,
		Outcome:  "complete",
		Output: `{"message_type":"summary","files_new":1,"dirs_new":1,"total_files_processed":1,` +
			`"total_bytes_processed":128,"data_added":40,"data_added_packed":32,"snapshot_id":"` + strings.Repeat("f", 64) + `"}`,
	}
}

func (executor *schedulerExecutor) count() int {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	count := 0
	for _, request := range executor.requests {
		if request.Operation != "stats" {
			count++
		}
	}
	return count
}

func TestSchedulerPersistsExactOfflineOccurrenceAndDeduplicatesClockMovement(t *testing.T) {
	store := newAgentTestStore(t)
	root := t.TempDir()
	job := executionJob(root)
	job.Schedule = JobSchedule{Kind: "cron", Expression: "*/5 * * * *", Timezone: "UTC"}
	config := schedulerConfig(job)
	now := time.Date(2026, 3, 29, 0, 59, 0, 0, time.UTC)
	executor := &schedulerExecutor{}
	runtime := newSchedulerDaemon(t, store, config, &now, executor)
	runtime.state.ClockOffsetSeconds = 421

	now = time.Date(2026, 3, 29, 1, 0, 0, 0, time.UTC)
	if err := runtime.scheduleBackups(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForScheduledExecutions(t, executor, 1)
	waitForFinishedScheduledRun(t, runtime)

	persisted, err := store.LoadCommandJournal()
	if err != nil || len(persisted.Commands) != 1 {
		t.Fatalf("persisted scheduled run: %+v %v", persisted, err)
	}
	var scheduled *JournalCommand
	for _, command := range persisted.Commands {
		scheduled = command
	}
	if scheduled.Trigger != "scheduled" || scheduled.ScheduledFor != "2026-03-29T01:00:00.000000Z" || scheduled.ConfigRevision != 2 {
		t.Fatalf("scheduled identity: %+v", scheduled)
	}
	if len(scheduled.Events) != 3 || scheduled.Events[0].ConfigRevision != 2 || scheduled.Events[0].Trigger != "scheduled" || scheduled.Events[0].ScheduledFor != scheduled.ScheduledFor {
		t.Fatalf("scheduled event envelope: %+v", scheduled.Events)
	}
	if scheduled.Events[1].Kind != "clock_skew" || scheduled.Events[1].Payload["offset_seconds"] != float64(421) && scheduled.Events[1].Payload["offset_seconds"] != int64(421) {
		t.Fatalf("clock skew evidence: %+v", scheduled.Events[1])
	}
	databaseBytes, err := os.ReadFile(store.databasePath())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(databaseBytes, []byte(job.Repository.ServicePassword)) {
		t.Fatal("durable state contains the plaintext repository password")
	}

	now = time.Date(2026, 3, 29, 0, 59, 0, 0, time.UTC)
	if err := runtime.scheduleBackups(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = time.Date(2026, 3, 29, 1, 0, 0, 0, time.UTC)
	if err := runtime.scheduleBackups(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = time.Date(2026, 3, 29, 1, 11, 0, 0, time.UTC)
	if err := runtime.scheduleBackups(context.Background()); err != nil {
		t.Fatal(err)
	}
	if executor.count() != 1 {
		t.Fatalf("clock movement repeated or caught up executions: %d", executor.count())
	}

	restarted := newSchedulerDaemon(t, store, config, &now, executor)
	restarted.lastScheduleMinute = time.Date(2026, 3, 29, 0, 59, 0, 0, time.UTC)
	now = time.Date(2026, 3, 29, 1, 0, 0, 0, time.UTC)
	if err := restarted.scheduleBackups(context.Background()); err != nil {
		t.Fatal(err)
	}
	if executor.count() != 1 {
		t.Fatalf("restart repeated a durable occurrence: %d", executor.count())
	}
}

func TestSchedulerRecordsOverlapInsteadOfWaitingPastTheOccurrence(t *testing.T) {
	store := newAgentTestStore(t)
	release := make(chan struct{})
	executor := &schedulerExecutor{release: release}
	now := time.Date(2026, 10, 25, 0, 59, 0, 0, time.UTC)
	jobs := make([]Job, 3)
	ids := []string{
		"01k4p4f7m1r9d3t6v8w2x5y7za",
		"01k4p4f7m1r9d3t6v8w2x5y7zb",
		"01k4p4f7m1r9d3t6v8w2x5y7zc",
	}
	for index := range jobs {
		jobs[index] = executionJob(t.TempDir())
		jobs[index].ID = ids[index]
		jobs[index].Repository.ID = strings.Repeat(string(rune('a'+index)), 64)
		jobs[index].Schedule = JobSchedule{Kind: "preset", Preset: "h24", Expression: "0 1 * * *", Timezone: "UTC"}
	}
	runtime := newSchedulerDaemon(t, store, Config{
		ProtocolRevision: ProtocolRevision, Generation: 1, Revision: 2, SchemaVersion: ConfigSchemaVersion,
		IssuedAt: "2026-10-25T00:55:00.000000Z", Jobs: jobs,
	}, &now, executor)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	now = time.Date(2026, 10, 25, 1, 0, 0, 0, time.UTC)
	if err := runtime.scheduleBackups(ctx); err != nil {
		t.Fatal(err)
	}
	waitForScheduledExecutions(t, executor, 2)

	runtime.mu.Lock()
	overlaps := 0
	for _, command := range runtime.journal.Commands {
		if command.Result != nil && command.Result.ResultCode == "schedule_overlap" {
			overlaps++
			if !command.ResultReported || len(command.Events) != 1 {
				t.Fatalf("overlap evidence: %+v", command)
			}
		}
	}
	runtime.mu.Unlock()
	if overlaps != 1 {
		t.Fatalf("overlap runs: %d", overlaps)
	}
	close(release)
	waitForAllScheduledRuns(t, runtime, 3)
}

func TestOfflineRunsContinueDuringSlowReportingAndReplayAfterRestart(t *testing.T) {
	store := newAgentTestStore(t)
	job := executionJob(t.TempDir())
	job.Schedule = JobSchedule{Kind: "cron", Expression: "*/5 * * * *", Timezone: "UTC"}
	config := schedulerConfig(job)
	now := time.Date(2026, 9, 10, 0, 59, 0, 0, time.UTC)
	executor := &schedulerExecutor{}
	runtime := newSchedulerDaemon(t, store, config, &now, executor)

	now = time.Date(2026, 9, 10, 1, 0, 0, 0, time.UTC)
	if err := runtime.scheduleBackups(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForScheduledExecutions(t, executor, 1)
	waitForFinishedScheduledRun(t, runtime)

	uploadStarted := make(chan struct{})
	releaseUpload := make(chan struct{})
	var uploadOnce sync.Once
	var receivedMu sync.Mutex
	receivedRuns := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		switch {
		case request.URL.Path == "/agent/v1/events":
			uploadOnce.Do(func() {
				close(uploadStarted)
				<-releaseUpload
			})
			var eventRequest EventRequest
			if err := json.NewDecoder(request.Body).Decode(&eventRequest); err != nil {
				t.Errorf("decode replay events: %v", err)
			}
			results := make([]EventResult, 0, len(eventRequest.Events))
			receivedMu.Lock()
			for _, event := range eventRequest.Events {
				receivedRuns[event.RunID] = true
				results = append(results, EventResult{ID: event.ID, Status: "accepted"})
			}
			receivedMu.Unlock()
			_ = json.NewEncoder(response).Encode(EventsResponse{ProtocolRevision: ProtocolRevision, Results: results})
		case strings.Contains(request.URL.Path, "/logs/") && strings.Contains(request.URL.Path, "/chunks/"):
			_, _ = io.Copy(io.Discard, request.Body)
			response.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(request.URL.Path, "/complete"):
			response.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(request.URL.Path, "/result"):
			t.Error("scheduled runs submitted a manual command result")
			response.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()
	runtime.client = NewClient(server.URL+"/agent/v1", testBootstrap().Credential, "1.1.0", server.Client())

	reportDone := make(chan error, 1)
	go func() { reportDone <- runtime.reportJournal(context.Background()) }()
	select {
	case <-uploadStarted:
	case <-time.After(time.Second):
		t.Fatal("reporter did not begin the slow upload")
	}

	now = time.Date(2026, 9, 10, 1, 5, 0, 0, time.UTC)
	if err := runtime.scheduleBackups(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForScheduledExecutions(t, executor, 2)
	close(releaseUpload)
	if err := <-reportDone; err != nil {
		t.Fatal(err)
	}

	restarted := newSchedulerDaemon(t, store, config, &now, executor)
	restarted.client = NewClient(server.URL+"/agent/v1", testBootstrap().Credential, "1.1.0", server.Client())
	if err := restarted.reportJournal(context.Background()); err != nil {
		t.Fatal(err)
	}
	persisted, err := store.LoadCommandJournal()
	if err != nil || len(persisted.Commands) != 0 {
		t.Fatalf("replayed journal: %+v %v", persisted, err)
	}
	receivedMu.Lock()
	defer receivedMu.Unlock()
	if len(receivedRuns) != 2 || executor.count() != 2 {
		t.Fatalf("offline replay: runs=%d executions=%d", len(receivedRuns), executor.count())
	}
}

func TestSpoolPressureReleasesReserveAndRecordsAnOccurrenceGap(t *testing.T) {
	store := newAgentTestStore(t)
	store.reserveBytes = 4096
	if _, err := store.LoadRuntimeState(); err != nil {
		t.Fatal(err)
	}
	store.spoolLimit = store.spoolUsage()
	err := store.WriteRunLog("01k4p4f7m1r9d3t6v8w2x5y7za", []byte("synthetic bounded log"))
	if !errors.Is(err, ErrSpoolCapacity) {
		t.Fatalf("spool pressure error: %v", err)
	}
	if _, err := os.Stat(store.reservePath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("emergency reserve was not released: %v", err)
	}
	if store.canAcceptWork(0) {
		t.Fatal("new work remained enabled after the emergency reserve was released")
	}

	pressureStore := newAgentTestStore(t)
	job := executionJob(t.TempDir())
	job.Schedule = JobSchedule{Kind: "cron", Expression: "*/5 * * * *", Timezone: "UTC"}
	now := time.Date(2026, 9, 10, 1, 4, 0, 0, time.UTC)
	executor := &schedulerExecutor{}
	runtime := newSchedulerDaemon(t, pressureStore, schedulerConfig(job), &now, executor)
	pressureStore.spoolLimit = 1
	now = time.Date(2026, 9, 10, 1, 5, 0, 0, time.UTC)
	if err := runtime.scheduleBackups(context.Background()); err != nil {
		t.Fatal(err)
	}
	if executor.count() != 0 || len(runtime.journal.Commands) != 0 {
		t.Fatal("spool pressure created work")
	}
	exists, err := pressureStore.occurrenceExists(job.ID, "backup", protocolTimestamp(now))
	if err != nil || !exists {
		t.Fatalf("pressure occurrence tombstone: found=%t err=%v", exists, err)
	}
	state, err := pressureStore.LoadRuntimeState()
	if err != nil || !state.SpoolGapDetected {
		t.Fatalf("pressure gap state: %+v %v", state, err)
	}
}

func TestStateDatabaseImportsLegacyJSONOnceAndCreatesEveryV1Bucket(t *testing.T) {
	store := newAgentTestStore(t)
	if err := os.MkdirAll(filepath.Dir(store.Paths.State), 0o700); err != nil {
		t.Fatal(err)
	}
	state := RuntimeState{AuthenticationPaused: true}
	stateData, _ := json.Marshal(state)
	if err := os.WriteFile(store.Paths.State, stateData, 0o600); err != nil {
		t.Fatal(err)
	}
	command := AgentCommand{
		ID: "01k4p4f7m1r9d3t6v8w2x5y7za", Generation: 1, Kind: "run_backup",
		Payload: CommandPayload{JobID: "01k4p4f7m1r9d3t6v8w2x5y7zb", RequiredConfigRevision: 2},
	}
	legacy := CommandJournal{Version: 1, Commands: map[string]*JournalCommand{
		command.ID: {Command: command, RunID: "01k4p4f7m1r9d3t6v8w2x5y7zc", State: "received", Events: []AgentEvent{}},
	}}
	journalData, _ := json.Marshal(legacy)
	if err := os.WriteFile(store.commandsPath(), journalData, 0o600); err != nil {
		t.Fatal(err)
	}

	loadedState, err := store.LoadRuntimeState()
	if err != nil || !loadedState.AuthenticationPaused {
		t.Fatalf("imported runtime: %+v %v", loadedState, err)
	}
	loadedJournal, err := store.LoadCommandJournal()
	if err != nil || loadedJournal.Commands[command.ID].Trigger != "manual" {
		t.Fatalf("imported command: %+v %v", loadedJournal, err)
	}
	for _, path := range []string{store.Paths.State, store.commandsPath()} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("legacy file was retained: %s", path)
		}
	}

	database, err := bolt.Open(store.databasePath(), 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.View(func(transaction *bolt.Tx) error {
		for _, name := range [][]byte{metadataBucket, commandsBucket, runsBucket, eventsBucket, occurrencesBucket, logRefsBucket} {
			if transaction.Bucket(name) == nil {
				t.Fatalf("missing bucket %q", name)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestStateDatabaseBackfillsRunKindForDurableBackupRecords(t *testing.T) {
	store := newAgentTestStore(t)
	commandID := "01k4p4f7m1r9d3t6v8w2x5y7za"
	runID := "01k4p4f7m1r9d3t6v8w2x5y7zb"
	jobID := "01k4p4f7m1r9d3t6v8w2x5y7zc"
	eventID := "01k4p4f7m1r9d3t6v8w2x5y7zd"
	journal := newCommandJournal()
	journal.Commands[commandID] = &JournalCommand{
		Command: AgentCommand{
			ID: commandID, Generation: 1, Kind: "run_backup",
			Payload: CommandPayload{JobID: jobID, RequiredConfigRevision: 3},
		},
		RunID: runID, ConfigRevision: 3, Trigger: "scheduled", ScheduledFor: "2026-09-10T04:10:00.000000Z",
		State: "finished", Sequence: 1,
		Events: []AgentEvent{{
			ID: eventID, RunID: runID, JobID: jobID, ConfigRevision: 3, Trigger: "scheduled",
			ScheduledFor: "2026-09-10T04:10:00.000000Z", Sequence: 1,
			OccurredAt: "2026-09-10T04:10:01.000000Z", Kind: "run_finished", Payload: map[string]any{},
		}},
		Result: &CommandResult{
			Generation: 1, RunID: runID, JobID: jobID, Status: "complete", ResultCode: "success",
			StartedAt: "2026-09-10T04:10:00.000000Z", FinishedAt: "2026-09-10T04:10:01.000000Z", SnapshotIDs: []string{},
		},
	}
	if err := store.SaveCommandJournal(journal); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.LoadCommandJournal()
	if err != nil {
		t.Fatal(err)
	}
	command := loaded.Commands[commandID]
	if command.RunKind != "backup" || command.Events[0].RunKind != "backup" || command.Result.RunKind != "backup" {
		t.Fatalf("legacy run kinds were not backfilled: %+v", command)
	}
	if err := store.SaveCommandJournal(loaded); err != nil {
		t.Fatal(err)
	}
	reloaded, err := store.LoadCommandJournal()
	if err != nil || reloaded.Commands[commandID].Events[0].RunKind != "backup" {
		t.Fatalf("backfilled event was not durable: %+v %v", reloaded, err)
	}
}

func TestStateDatabaseCompactsAcknowledgedVerboseRecords(t *testing.T) {
	store := newAgentTestStore(t)
	journal := newCommandJournal()
	now := time.Date(2026, 9, 10, 3, 0, 0, 0, time.UTC)
	for range 200 {
		commandID, err := newULID(now)
		if err != nil {
			t.Fatal(err)
		}
		runID, err := newULID(now)
		if err != nil {
			t.Fatal(err)
		}
		eventID, err := newULID(now)
		if err != nil {
			t.Fatal(err)
		}
		journal.Commands[commandID] = &JournalCommand{
			Command: AgentCommand{ID: commandID, Generation: 1, Kind: "run_backup", Payload: CommandPayload{
				JobID: "01k4p4f7m1r9d3t6v8w2x5y7za", RequiredConfigRevision: 2,
			}},
			RunID: runID, ConfigRevision: 2, Trigger: "manual", ReceivedAt: protocolTimestamp(now),
			Acknowledged: true, State: "running", Sequence: 1,
			Events: []AgentEvent{{
				ID: eventID, RunID: runID, JobID: "01k4p4f7m1r9d3t6v8w2x5y7za", ConfigRevision: 2,
				Trigger: "manual", Sequence: 1, OccurredAt: protocolTimestamp(now), Kind: "run_progress",
				Payload: map[string]any{"synthetic_detail": strings.Repeat("x", 2048)},
			}},
		}
	}
	if err := store.SaveCommandJournal(journal); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCommandJournal(newCommandJournal()); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(store.databasePath())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.compactStateDatabase(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(store.databasePath())
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() >= before.Size() {
		t.Fatalf("state database did not compact: before=%d after=%d", before.Size(), after.Size())
	}
	loaded, err := store.LoadCommandJournal()
	if err != nil || len(loaded.Commands) != 0 {
		t.Fatalf("compacted state is invalid: %+v %v", loaded, err)
	}
}

func TestStateDatabaseKeepsBackupAndCancellationCommandsForTheSameRun(t *testing.T) {
	store := newAgentTestStore(t)
	runID := "01k4p4f7m1r9d3t6v8w2x5y7za"
	backupID := "01k4p4f7m1r9d3t6v8w2x5y7zb"
	cancelID := "01k4p4f7m1r9d3t6v8w2x5y7zc"
	journal := newCommandJournal()
	journal.Commands[backupID] = &JournalCommand{
		Command: AgentCommand{ID: backupID, Generation: 1, Kind: "run_backup", Payload: CommandPayload{
			JobID: "01k4p4f7m1r9d3t6v8w2x5y7zd", RequiredConfigRevision: 2,
		}},
		RunID: runID, ConfigRevision: 2, Trigger: "manual", State: "running", Sequence: 1,
		Events: []AgentEvent{{
			ID: "01k4p4f7m1r9d3t6v8w2x5y7ze", RunID: runID, JobID: "01k4p4f7m1r9d3t6v8w2x5y7zd",
			ConfigRevision: 2, Trigger: "manual", Sequence: 1, Kind: "run_started", Payload: map[string]any{},
		}},
	}
	journal.Commands[cancelID] = &JournalCommand{
		Command: AgentCommand{ID: cancelID, Generation: 1, Kind: "cancel_run", Payload: CommandPayload{
			JobID: "01k4p4f7m1r9d3t6v8w2x5y7zd", RunID: runID,
		}},
		RunID: runID, ConfigRevision: 2, Trigger: "manual", State: "received", Events: []AgentEvent{},
	}
	if err := store.SaveCommandJournal(journal); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadCommandJournal()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Commands) != 2 || len(loaded.Commands[backupID].Events) != 1 || len(loaded.Commands[cancelID].Events) != 0 {
		t.Fatalf("shared-run commands were conflated: %+v", loaded)
	}
}

func schedulerConfig(job Job) Config {
	return Config{
		ProtocolRevision: ProtocolRevision, Generation: 1, Revision: 2, SchemaVersion: ConfigSchemaVersion,
		IssuedAt: "2026-09-10T00:00:00.000000Z", Jobs: []Job{job},
	}
}

func newSchedulerDaemon(t *testing.T, store *FileStore, config Config, now *time.Time, executor BackupExecutor) *daemon {
	t.Helper()
	bootstrap := testBootstrap()
	journal, err := store.LoadCommandJournal()
	if err != nil {
		t.Fatal(err)
	}
	return &daemon{
		store: store, bootstrap: bootstrap, now: func() time.Time { return *now },
		metadata: ConfigMetadata{Generation: 1, Revision: config.Revision, Digest: strings.Repeat("a", 64)},
		config:   config, journal: journal, executor: executor,
		active: map[string]context.CancelFunc{}, repositories: map[string]bool{},
		lastScheduleMinute: now.UTC().Truncate(time.Minute),
	}
}

func waitForScheduledExecutions(t *testing.T, executor *schedulerExecutor, expected int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if executor.count() == expected {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("scheduled executions: got %d, want %d", executor.count(), expected)
}

func waitForFinishedScheduledRun(t *testing.T, runtime *daemon) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		runtime.mu.Lock()
		finished := false
		for _, command := range runtime.journal.Commands {
			finished = command.State == "finished"
		}
		runtime.mu.Unlock()
		if finished {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("scheduled run did not finish")
}

func waitForAllScheduledRuns(t *testing.T, runtime *daemon, expected int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		runtime.mu.Lock()
		finished := len(runtime.journal.Commands) == expected && len(runtime.active) == 0
		for _, command := range runtime.journal.Commands {
			finished = finished && command.State == "finished"
		}
		runtime.mu.Unlock()
		if finished {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("scheduled runs did not reach durable terminal state")
}
