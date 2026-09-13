package agent

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/chieftools/backupchief-agent/restic"
)

type recordingExecutor struct {
	requests []restic.Request
	result   restic.Result
}

type blockingExecutor struct {
	started chan struct{}
	stopped chan struct{}
}

func (executor *blockingExecutor) Run(ctx context.Context, _ restic.Request) restic.Result {
	close(executor.started)
	<-ctx.Done()
	close(executor.stopped)
	return restic.Result{ExitCode: 1, Outcome: "cancelled", Diagnostic: "synthetic cancellation"}
}

func (executor *recordingExecutor) Run(_ context.Context, request restic.Request) restic.Result {
	executor.requests = append(executor.requests, request)
	return executor.result
}

func TestExecuteBackupPassesLiteralSelectionAndParsesSummary(t *testing.T) {
	root := filepath.Join(t.TempDir(), "source with spaces; literal")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	snapshotID := strings.Repeat("a", 64)
	executor := &recordingExecutor{result: restic.Result{
		ExitCode: 0,
		Outcome:  "complete",
		Output:   `{"message_type":"summary","files_new":2,"files_changed":1,"files_unmodified":3,"dirs_new":1,"dirs_changed":0,"dirs_unmodified":2,"total_files_processed":6,"total_bytes_processed":4096,"data_added":1536,"data_added_packed":1024,"snapshot_id":"` + snapshotID + `"}`,
	}}
	command := &JournalCommand{
		RunID:   "01k4p4f7m1r9d3t6v8w2x5y7zc",
		Command: AgentCommand{Payload: CommandPayload{JobID: "01k4p4f7m1r9d3t6v8w2x5y7zd"}},
	}
	job := executionJob(root)
	now := time.Date(2026, 7, 8, 9, 10, 11, 0, time.UTC)

	result, _, _, _ := executeBackup(context.Background(), executor, t.TempDir(), "01k4p4f7m1r9d3t6v8w2x5y7ze", 1, command, job, func() time.Time { return now })

	if result.Status != "complete" || result.ResultCode != "success" || result.Statistics == nil || (*result.Statistics)["source_files"] != uint64(6) || (*result.Statistics)["stored_bytes"] != uint64(1024) || !reflect.DeepEqual(result.SnapshotIDs, []string{snapshotID}) {
		t.Fatalf("result: %+v", result)
	}
	if len(executor.requests) != 1 {
		t.Fatalf("requests: %d", len(executor.requests))
	}
	request := executor.requests[0]
	if request.Root != root || !reflect.DeepEqual(request.Excludes, job.Source.Excludes) || request.Host != "01k4p4f7m1r9d3t6v8w2x5y7ze" {
		t.Fatalf("literal request changed: %+v", request)
	}
	wantTags := []string{"backupchief-job:" + job.ID, "backupchief-run:" + command.RunID}
	if !reflect.DeepEqual(request.Tags, wantTags) {
		t.Fatalf("tags: %v", request.Tags)
	}
}

func TestMeasureRepositoryBytesUsesRawDataStats(t *testing.T) {
	executor := &recordingExecutor{result: restic.Result{
		ExitCode: 0,
		Outcome:  "complete",
		Output:   `{"total_size":8192,"total_file_count":4}`,
	}}

	repositoryBytes := measureRepositoryBytes(context.Background(), executor, executionJob(t.TempDir()))

	if repositoryBytes == nil || *repositoryBytes != 8192 {
		t.Fatalf("repository bytes: %v", repositoryBytes)
	}
	if len(executor.requests) != 1 || executor.requests[0].Operation != "stats" {
		t.Fatalf("requests: %+v", executor.requests)
	}
}

func TestExecuteMySQLBackupStreamsOneDatabaseIntoRestic(t *testing.T) {
	snapshotID := strings.Repeat("d", 64)
	executor := &recordingExecutor{result: restic.Result{ExitCode: 0, Outcome: "complete", Output: `{"message_type":"summary","total_files_processed":1,"total_bytes_processed":2048,"data_added_packed":512,"snapshot_id":"` + snapshotID + `"}`}}
	command := &JournalCommand{RunID: "01k4p4f7m1r9d3t6v8w2x5y7zc"}
	job := executionJob(t.TempDir())
	job.Type = JobTypeMySQL
	job.Source = JobSource{MySQL: &MySQLSource{Host: "mysql.example.test", Port: 3306, Username: "synthetic_reader", Password: "synthetic-secret", SelectionMode: "selected", Databases: []string{"synthetic_app"}, CustomFlags: []string{"--hex-blob"}}}
	now := func() time.Time { return time.Date(2026, 9, 13, 8, 30, 0, 0, time.UTC) }

	result, _, _, _ := executeBackup(context.Background(), executor, t.TempDir(), "01k4p4f7m1r9d3t6v8w2x5y7ze", 1, command, job, now)

	if result.Status != "complete" || result.ResultCode != "success" || len(result.Artifacts) != 1 || result.Artifacts[0].Database != "synthetic_app" || result.Artifacts[0].Filename != "synthetic_app.sql" || result.Artifacts[0].SnapshotID != snapshotID {
		t.Fatalf("result: %+v", result)
	}
	if len(executor.requests) != 1 {
		t.Fatalf("requests: %d", len(executor.requests))
	}
	request := executor.requests[0]
	if request.Operation != "backup_stdin" || request.StdinFilename != "synthetic_app.sql" || !strings.Contains(request.CommandConfig, `password="synthetic-secret"`) {
		t.Fatalf("stream request: %+v", request)
	}
	if !contains(request.Tags, "backupchief-run-anchor") || !contains(request.Tags, "backupchief-database:73796e7468657469635f617070") {
		t.Fatalf("tags: %v", request.Tags)
	}
	for _, argument := range request.StdinCommand {
		if strings.Contains(argument, "synthetic-secret") {
			t.Fatalf("password leaked into argv: %v", request.StdinCommand)
		}
	}
}

func TestMySQLDumpFilenameFallsBackForNonPortableNames(t *testing.T) {
	filename := mysqlDumpFilename("synthetic schema")

	if !strings.HasPrefix(filename, "~") || !strings.HasSuffix(filename, ".sql") || len(filename) != 69 {
		t.Fatalf("filename: %q", filename)
	}
}

func TestExecuteBackupClassifiesEmptyPartialAndInvalidRoots(t *testing.T) {
	root := t.TempDir()
	snapshotID := strings.Repeat("b", 64)
	command := &JournalCommand{
		RunID:   "01k4p4f7m1r9d3t6v8w2x5y7zc",
		Command: AgentCommand{Payload: CommandPayload{JobID: "01k4p4f7m1r9d3t6v8w2x5y7zd"}},
	}
	now := func() time.Time { return time.Date(2026, 7, 8, 9, 10, 11, 0, time.UTC) }

	for _, test := range []struct {
		name       string
		exitCode   int
		files      int
		wantStatus string
		wantCode   string
	}{
		{name: "empty", exitCode: 0, files: 0, wantStatus: "complete", wantCode: "empty_selection"},
		{name: "partial", exitCode: 3, files: 2, wantStatus: "partial", wantCode: "unreadable_files"},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor := &recordingExecutor{result: restic.Result{
				ExitCode: test.exitCode,
				Outcome:  "complete",
				Output:   `{"message_type":"summary","total_files_processed":` + string(rune('0'+test.files)) + `,"snapshot_id":"` + snapshotID + `"}`,
			}}
			result, _, _, _ := executeBackup(context.Background(), executor, t.TempDir(), command.RunID, 1, command, executionJob(root), now)
			if result.Status != test.wantStatus || result.ResultCode != test.wantCode {
				t.Fatalf("result: %+v", result)
			}
		})
	}

	missing := filepath.Join(t.TempDir(), "missing")
	executor := &recordingExecutor{}
	result, _, _, _ := executeBackup(context.Background(), executor, t.TempDir(), command.RunID, 1, command, executionJob(missing), now)
	if result.ResultCode != "invalid_root" || len(executor.requests) != 0 {
		t.Fatalf("missing root: %+v requests=%d", result, len(executor.requests))
	}

	target := t.TempDir()
	symlink := filepath.Join(t.TempDir(), "root-link")
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatal(err)
	}
	result, _, _, _ = executeBackup(context.Background(), executor, t.TempDir(), command.RunID, 1, command, executionJob(symlink), now)
	if result.ResultCode != "invalid_root" || len(executor.requests) != 0 {
		t.Fatalf("symlink root: %+v requests=%d", result, len(executor.requests))
	}
}

func TestCommandReceiptReusesRunIdentityAndRejectsChangedReplay(t *testing.T) {
	store := newAgentTestStore(t)
	daemon := &daemon{store: store, now: time.Now, journal: newCommandJournal()}
	command := AgentCommand{
		ID: "01k4p4f7m1r9d3t6v8w2x5y7zc", Generation: 1, Kind: "run_backup",
		IssuedAt: "2026-07-08T09:10:11.000000Z", ExpiresAt: "2026-07-08T09:25:11.000000Z",
		Payload: CommandPayload{JobID: "01k4p4f7m1r9d3t6v8w2x5y7zd", RequiredConfigRevision: 2},
	}
	if err := daemon.receiveCommand(command); err != nil {
		t.Fatal(err)
	}
	firstRunID := daemon.journal.Commands[command.ID].RunID
	if err := daemon.receiveCommand(command); err != nil {
		t.Fatal(err)
	}
	if daemon.journal.Commands[command.ID].RunID != firstRunID || len(daemon.journal.Commands) != 1 {
		t.Fatalf("identity changed: %+v", daemon.journal)
	}
	command.Payload.RequiredConfigRevision = 3
	if err := daemon.receiveCommand(command); err == nil {
		t.Fatal("changed command replay was accepted")
	}
	persisted, err := store.LoadCommandJournal()
	if err != nil || persisted.Commands[command.ID].RunID != firstRunID {
		t.Fatalf("persisted journal: %+v %v", persisted, err)
	}
}

func TestCancelRunStopsTheActiveExecutionAndPersistsItsResult(t *testing.T) {
	store := newAgentTestStore(t)
	runID := "01k4p4f7m1r9d3t6v8w2x5y7zc"
	commandID := "01k4p4f7m1r9d3t6v8w2x5y7zd"
	job := executionJob(t.TempDir())
	executor := &blockingExecutor{started: make(chan struct{}), stopped: make(chan struct{})}
	journaled := &JournalCommand{
		Command: AgentCommand{
			ID: commandID, Generation: 1, Kind: "run_backup",
			Payload: CommandPayload{JobID: job.ID, RequiredConfigRevision: 1},
		},
		RunID: runID, ConfigRevision: 1, Trigger: "manual", ReceivedAt: protocolTimestamp(time.Now()), Acknowledged: true,
		State: "received", Events: []AgentEvent{},
	}
	bootstrap := testBootstrap()
	bootstrap.ServerID = runID
	daemon := &daemon{
		store: store, bootstrap: bootstrap, now: time.Now,
		journal:  CommandJournal{Version: commandJournalVersion, Commands: map[string]*JournalCommand{commandID: journaled}},
		executor: executor, active: map[string]context.CancelFunc{}, repositories: map[string]bool{},
	}
	if err := daemon.startBackup(context.Background(), commandID, job); err != nil {
		t.Fatal(err)
	}
	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("backup did not start")
	}
	if err := daemon.cancelRun(runID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-executor.stopped:
	case <-time.After(time.Second):
		t.Fatal("backup did not stop")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		daemon.mu.Lock()
		result := daemon.journal.Commands[commandID].Result
		daemon.mu.Unlock()
		if result != nil {
			if result.Status != "cancelled" || result.ResultCode != "cancelled" {
				t.Fatalf("result: %+v", result)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("cancelled result was not persisted")
}

func TestCancelRunFinishesAnAcknowledgedRunBeforeExecution(t *testing.T) {
	store := newAgentTestStore(t)
	runID := "01k4p4f7m1r9d3t6v8w2x5y7zc"
	commandID := "01k4p4f7m1r9d3t6v8w2x5y7zd"
	journaled := &JournalCommand{
		Command: AgentCommand{
			ID: commandID, Generation: 1, Kind: "run_backup",
			Payload: CommandPayload{JobID: "01k4p4f7m1r9d3t6v8w2x5y7ze", RequiredConfigRevision: 2},
		},
		RunID: runID, ConfigRevision: 2, Trigger: "manual", ReceivedAt: protocolTimestamp(time.Now()), Acknowledged: true,
		State: "received", Events: []AgentEvent{},
	}
	daemon := &daemon{
		store: store, bootstrap: Bootstrap{Generation: 1}, now: time.Now,
		journal: CommandJournal{Version: commandJournalVersion, Commands: map[string]*JournalCommand{
			commandID: journaled,
		}},
		active: map[string]context.CancelFunc{},
	}

	if err := daemon.cancelRun(runID); err != nil {
		t.Fatal(err)
	}

	result := journaled.Result
	if journaled.State != "finished" || result == nil || result.Status != "cancelled" || result.ResultCode != "cancelled" || len(journaled.Events) != 1 {
		t.Fatalf("cancelled command: %+v", journaled)
	}
	persisted, err := store.LoadCommandJournal()
	if err != nil || persisted.Commands[commandID].Result == nil || persisted.Commands[commandID].Result.ResultCode != "cancelled" {
		t.Fatalf("persisted cancellation: %+v %v", persisted, err)
	}
}

func executionJob(root string) Job {
	return Job{
		ID:      "01k4p4f7m1r9d3t6v8w2x5y7zd",
		Type:    JobTypeFile,
		Enabled: true,
		Source: JobSource{
			Root: root, OneFileSystem: true,
			Excludes: []string{"*.synthetic-cache", "nested/private example.test/*", "[literal]*"},
		},
		Repository: JobRepository{
			ID: strings.Repeat("c", 64), ServicePassword: "synthetic-service-password",
			Connection: RepositoryConnection{Driver: "local", Path: filepath.Join(root, "repository")},
		},
	}
}
