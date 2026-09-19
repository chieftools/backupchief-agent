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

type sequentialExecutor struct {
	requests []restic.Request
	results  []restic.Result
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

func (executor *sequentialExecutor) Run(_ context.Context, request restic.Request) restic.Result {
	executor.requests = append(executor.requests, request)
	result := executor.results[0]
	executor.results = executor.results[1:]
	return result
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
	installInspectionTools(t, successfulMySQLTool, successfulMySQLDumpTool)
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
	if request.Operation != "backup_stdin" || request.StdinFilename != "synthetic_app.sql" || !strings.Contains(request.CommandConfig, `password="synthetic-secret"`) || !contains(request.StdinCommand, "--set-gtid-purged=OFF") {
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

func TestMySQLBackupHonorsExplicitGTIDDumpOption(t *testing.T) {
	installInspectionTools(t, successfulMySQLTool, successfulMySQLDumpTool)
	snapshotID := strings.Repeat("c", 64)
	executor := &recordingExecutor{result: restic.Result{ExitCode: 0, Outcome: "complete", Output: `{"message_type":"summary","total_files_processed":1,"snapshot_id":"` + snapshotID + `"}`}}
	command := &JournalCommand{RunID: "01k4p4f7m1r9d3t6v8w2x5y7zc"}
	job := executionJob(t.TempDir())
	job.Type = JobTypeMySQL
	job.Source = JobSource{MySQL: &MySQLSource{Host: "mysql.example.test", Port: 3306, Username: "synthetic_reader", SelectionMode: "selected", Databases: []string{"synthetic_app"}, CustomFlags: []string{"--set-gtid-purged=ON"}}}

	executeBackup(context.Background(), executor, t.TempDir(), "01k4p4f7m1r9d3t6v8w2x5y7ze", 1, command, job, time.Now)

	arguments := executor.requests[0].StdinCommand
	if !contains(arguments, "--set-gtid-purged=ON") || contains(arguments, "--set-gtid-purged=OFF") {
		t.Fatalf("GTID options: %v", arguments)
	}
}

func TestMySQLBackupOmitsUnsupportedGTIDDumpOption(t *testing.T) {
	installInspectionTools(t, successfulMySQLTool, successfulMySQLDumpWithoutGTIDTool)
	snapshotID := strings.Repeat("b", 64)
	executor := &recordingExecutor{result: restic.Result{ExitCode: 0, Outcome: "complete", Output: `{"message_type":"summary","total_files_processed":1,"snapshot_id":"` + snapshotID + `"}`}}
	command := &JournalCommand{RunID: "01k4p4f7m1r9d3t6v8w2x5y7zc"}
	job := executionJob(t.TempDir())
	job.Type = JobTypeMySQL
	job.Source = JobSource{MySQL: &MySQLSource{Host: "mysql.example.test", Port: 3306, Username: "synthetic_reader", SelectionMode: "selected", Databases: []string{"synthetic_app"}}}

	executeBackup(context.Background(), executor, t.TempDir(), "01k4p4f7m1r9d3t6v8w2x5y7ze", 1, command, job, time.Now)

	if contains(executor.requests[0].StdinCommand, "--set-gtid-purged=OFF") {
		t.Fatalf("unsupported GTID option: %v", executor.requests[0].StdinCommand)
	}
}

func TestMySQLBackupReportsBytesPerDatabaseSnapshot(t *testing.T) {
	first, second := strings.Repeat("a", 64), strings.Repeat("b", 64)
	executor := &sequentialExecutor{results: []restic.Result{
		{ExitCode: 0, Outcome: "complete", Output: `{"message_type":"summary","total_files_processed":1,"total_bytes_processed":21901679,"data_added_packed":165183,"snapshot_id":"` + first + `"}`},
		{ExitCode: 0, Outcome: "complete", Output: `{"message_type":"summary","total_files_processed":1,"total_bytes_processed":4096,"data_added_packed":1024,"snapshot_id":"` + second + `"}`},
	}}
	command := &JournalCommand{RunID: "01k4p4f7m1r9d3t6v8w2x5y7zc"}
	job := executionJob(t.TempDir())
	job.Type = JobTypeMySQL
	job.Source = JobSource{MySQL: &MySQLSource{Host: "mysql.example.test", Port: 3306, Username: "synthetic_reader", Password: "synthetic-secret", SelectionMode: "selected", Databases: []string{"synthetic_app", "synthetic_metrics"}}}
	now := func() time.Time { return time.Date(2026, 9, 13, 8, 30, 0, 0, time.UTC) }

	result, _, _, _ := executeBackup(context.Background(), executor, t.TempDir(), "01k4p4f7m1r9d3t6v8w2x5y7ze", 1, command, job, now)

	if len(result.Artifacts) != 2 {
		t.Fatalf("artifacts: %+v", result.Artifacts)
	}
	for index, want := range []struct{ source, stored uint64 }{{21901679, 165183}, {4096, 1024}} {
		artifact := result.Artifacts[index]
		if artifact.SourceBytes == nil || *artifact.SourceBytes != want.source || artifact.StoredBytes == nil || *artifact.StoredBytes != want.stored {
			t.Fatalf("artifact %d bytes: %+v", index, artifact)
		}
	}

	statistics := *result.Statistics
	if statistics["source_bytes"].(uint64) != 21905775 || statistics["stored_bytes"].(uint64) != 166207 {
		t.Fatalf("run statistics must stay the sum across snapshots: %+v", statistics)
	}
}

func TestMySQLBackupReportsEachCompletedDatabaseBeforeTheRunFinishes(t *testing.T) {
	completedSnapshotID := strings.Repeat("a", 64)
	executor := &sequentialExecutor{results: []restic.Result{
		{ExitCode: 0, Outcome: "complete", Output: `{"message_type":"summary","total_files_processed":1,"total_bytes_processed":8192,"data_added_packed":2048,"snapshot_id":"` + completedSnapshotID + `"}`},
		{ExitCode: 1, Outcome: "failed", Diagnostic: "synthetic database failure"},
	}}
	command := &JournalCommand{RunID: "01k4p4f7m1r9d3t6v8w2x5y7zc"}
	job := executionJob(t.TempDir())
	job.Type = JobTypeMySQL
	job.Source = JobSource{MySQL: &MySQLSource{Host: "mysql.example.test", Port: 3306, Username: "synthetic_reader", Password: "synthetic-secret", SelectionMode: "selected", Databases: []string{"synthetic_accounts", "synthetic_audit"}}}
	type progressRecord struct {
		artifact  BackupArtifact
		completed int
		total     int
	}
	progress := []progressRecord{}

	result, _, _, _ := executeBackup(
		context.Background(),
		executor,
		t.TempDir(),
		"01k4p4f7m1r9d3t6v8w2x5y7ze",
		1,
		command,
		job,
		time.Now,
		func(artifact BackupArtifact, completed, total int) {
			progress = append(progress, progressRecord{artifact: artifact, completed: completed, total: total})
		},
	)

	if result.Status != "partial" || len(progress) != 1 {
		t.Fatalf("result=%+v progress=%+v", result, progress)
	}
	if progress[0].artifact.SnapshotID != completedSnapshotID || progress[0].artifact.Database != "synthetic_accounts" || progress[0].completed != 1 || progress[0].total != 2 {
		t.Fatalf("progress: %+v", progress[0])
	}
}

func TestMySQLDumpFilenameFallsBackForNonPortableNames(t *testing.T) {
	filename := mysqlDumpFilename("synthetic schema")

	if !strings.HasPrefix(filename, "~") || !strings.HasSuffix(filename, ".sql") || len(filename) != 69 {
		t.Fatalf("filename: %q", filename)
	}
}

func TestMySQLTableSelectionBuildsExactIncludeAndExcludeArguments(t *testing.T) {
	include := MySQLSource{TableSelection: &TableSelection{
		Mode: "include",
		Tables: []TableSelectionEntry{
			{Database: "synthetic_app", Table: "orders"},
			{Database: "synthetic_other", Table: "ignored_here"},
			{Database: "synthetic_app", Table: "accounts"},
		},
	}}
	exclude := include
	exclude.TableSelection = &TableSelection{Mode: "exclude", Tables: include.TableSelection.Tables}

	if got, want := mysqlTableArguments(include, "synthetic_app"), []string{"synthetic_app", "accounts", "orders"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("include arguments: %v, want %v", got, want)
	}
	if got, want := mysqlTableArguments(exclude, "synthetic_app"), []string{"--ignore-table=synthetic_app.accounts", "--ignore-table=synthetic_app.orders", "--databases", "synthetic_app"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("exclude arguments: %v, want %v", got, want)
	}
}

func TestDatabaseExclusionsFilterDiscoveredTargets(t *testing.T) {
	mysql, mysqlValid := selectedMySQLDatabases(MySQLSource{
		SelectionMode: "exclude",
		Databases:     []string{"synthetic_scratch"},
	}, []string{"synthetic_app", "synthetic_scratch", "synthetic_store"})
	postgresql, postgresqlValid := selectedPostgreSQLDatabases(PostgreSQLSource{
		SelectionMode: "exclude",
		Databases:     []string{"synthetic_scratch"},
	}, []string{"postgres", "synthetic_app", "synthetic_scratch"})

	if !mysqlValid || !reflect.DeepEqual(mysql, []string{"synthetic_app", "synthetic_store"}) {
		t.Fatalf("MySQL targets: %v valid=%v", mysql, mysqlValid)
	}
	if !postgresqlValid || !reflect.DeepEqual(postgresql, []string{"postgres", "synthetic_app"}) {
		t.Fatalf("PostgreSQL targets: %v valid=%v", postgresql, postgresqlValid)
	}
}

func TestExecutePostgreSQLBackupUsesAPrivatePassfileAndPortableDumpFlags(t *testing.T) {
	installPostgreSQLInspectionTools(t, successfulPostgreSQLTool, successfulPostgreSQLDumpTool)
	snapshotID := strings.Repeat("e", 64)
	executor := &recordingExecutor{result: restic.Result{ExitCode: 0, Outcome: "complete", Output: `{"message_type":"summary","total_files_processed":1,"total_bytes_processed":3072,"data_added_packed":768,"snapshot_id":"` + snapshotID + `"}`}}
	command := &JournalCommand{RunID: "01k4p4f7m1r9d3t6v8w2x5y7zc"}
	job := executionJob(t.TempDir())
	job.Type = JobTypePostgreSQL
	job.Source = JobSource{PostgreSQL: &PostgreSQLSource{Host: "postgresql.example.test", Port: 5432, Username: "synthetic_reader", Password: "synthetic-secret", ConnectionDatabase: "postgres", SelectionMode: "selected", Databases: []string{"synthetic_app"}}}
	now := func() time.Time { return time.Date(2026, 9, 13, 9, 30, 0, 0, time.UTC) }
	progress := []BackupArtifact{}

	result, _, _, _ := executeBackup(
		context.Background(), executor, t.TempDir(), "01k4p4f7m1r9d3t6v8w2x5y7ze", 1, command, job, now,
		func(artifact BackupArtifact, completed, total int) {
			if completed != 1 || total != 1 {
				t.Fatalf("progress count: %d/%d", completed, total)
			}
			progress = append(progress, artifact)
		},
	)

	if result.Status != "complete" || result.ResultCode != "success" || len(result.Artifacts) != 1 || result.Artifacts[0].Filename != "synthetic_app.sql" || len(progress) != 1 || progress[0].SnapshotID != snapshotID {
		t.Fatalf("result: %+v", result)
	}
	if len(executor.requests) != 1 {
		t.Fatalf("requests: %d", len(executor.requests))
	}
	request := executor.requests[0]
	if !strings.Contains(request.CommandConfig, "synthetic-secret") || !contains(request.StdinCommand, "--no-owner") || !contains(request.StdinCommand, "--no-privileges") || !contains(request.StdinCommand, "--no-tablespaces") || !contains(request.StdinCommand, "--quote-all-identifiers") || !contains(request.Tags, "backupchief-run-anchor") {
		t.Fatalf("stream request: %+v", request)
	}
	for _, argument := range request.StdinCommand {
		if strings.Contains(argument, "synthetic-secret") || strings.Contains(argument, "--create") || strings.Contains(argument, "--clean") {
			t.Fatalf("unsafe pg_dump argument: %q", argument)
		}
	}
}

func TestPostgreSQLTableSelectionQuotesExactIdentifierPatterns(t *testing.T) {
	source := PostgreSQLSource{TableSelection: &TableSelection{
		Mode: "include",
		Tables: []TableSelectionEntry{
			{Database: "synthetic_app", Schema: `Case"Schema`, Table: "Order.Items"},
			{Database: "synthetic_other", Schema: "public", Table: "ignored_here"},
		},
	}}

	arguments := postgresqlDumpArguments(source, "synthetic_app", "/tmp/synthetic-passfile", false)
	if !contains(arguments, "--strict-names") || !contains(arguments, `--table="Case""Schema"."Order.Items"`) {
		t.Fatalf("include arguments: %v", arguments)
	}

	source.TableSelection.Mode = "exclude"
	arguments = postgresqlDumpArguments(source, "synthetic_app", "/tmp/synthetic-passfile", false)
	if contains(arguments, "--strict-names") || !contains(arguments, `--exclude-table="Case""Schema"."Order.Items"`) {
		t.Fatalf("exclude arguments: %v", arguments)
	}
}

func TestPostgreSQLRunAnchorMovesToTheFirstSuccessfulSnapshot(t *testing.T) {
	installPostgreSQLInspectionTools(t, successfulPostgreSQLTool, successfulPostgreSQLDumpTool)
	snapshotID := strings.Repeat("f", 64)
	executor := &sequentialExecutor{results: []restic.Result{
		{ExitCode: 1, Outcome: "failed", Output: "synthetic first database failure"},
		{ExitCode: 0, Outcome: "complete", Output: `{"message_type":"summary","total_files_processed":1,"total_bytes_processed":1024,"data_added_packed":256,"snapshot_id":"` + snapshotID + `"}`},
	}}
	job := executionJob(t.TempDir())
	job.Type = JobTypePostgreSQL
	job.Source = JobSource{PostgreSQL: &PostgreSQLSource{Host: "postgresql.example.test", Port: 5432, Username: "synthetic_reader", ConnectionDatabase: "postgres", SelectionMode: "selected", Databases: []string{"postgres", "synthetic_app"}}}
	command := &JournalCommand{RunID: "01k4p4f7m1r9d3t6v8w2x5y7zc"}

	result, _, _, _ := executeBackup(context.Background(), executor, t.TempDir(), "01k4p4f7m1r9d3t6v8w2x5y7ze", 1, command, job, time.Now)

	if result.Status != "partial" || len(result.SnapshotIDs) != 1 || len(executor.requests) != 2 || !contains(executor.requests[1].Tags, "backupchief-run-anchor") {
		t.Fatalf("result=%+v requests=%+v", result, executor.requests)
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

func TestNewCommandReceiptWakesDispatcher(t *testing.T) {
	store := newAgentTestStore(t)
	daemon := &daemon{
		store: store, now: time.Now, journal: newCommandJournal(),
		dispatchWake: make(chan struct{}, 1),
	}
	command := AgentCommand{
		ID: "01k4p4f7m1r9d3t6v8w2x5y7ze", Generation: 1, Kind: "run_backup",
		IssuedAt: "2026-07-08T09:10:11.000000Z", ExpiresAt: "2026-07-08T09:25:11.000000Z",
		Payload: CommandPayload{JobID: "01k4p4f7m1r9d3t6v8w2x5y7zf", RequiredConfigRevision: 2},
	}

	if err := daemon.receiveCommand(command); err != nil {
		t.Fatal(err)
	}
	select {
	case <-daemon.dispatchWake:
	default:
		t.Fatal("new command did not wake dispatcher")
	}

	if err := daemon.receiveCommand(command); err != nil {
		t.Fatal(err)
	}
	select {
	case <-daemon.dispatchWake:
		t.Fatal("unchanged command replay woke dispatcher")
	default:
	}
}

func TestDatabaseBackupProgressIsDurablyJournaledAndWakesReporter(t *testing.T) {
	store := newAgentTestStore(t)
	commandID := "01k4p4f7m1r9d3t6v8w2x5y7zd"
	journaled := &JournalCommand{
		Command: AgentCommand{
			ID: commandID, Generation: 1, Kind: "run_backup",
			Payload: CommandPayload{JobID: "01k4p4f7m1r9d3t6v8w2x5y7ze", RequiredConfigRevision: 2},
		},
		RunID: "01k4p4f7m1r9d3t6v8w2x5y7zc", ConfigRevision: 2, RunKind: "backup", Trigger: "manual",
		ReceivedAt: "2026-09-14T10:00:00.000000Z", Acknowledged: true, State: "running", Sequence: 1, Events: []AgentEvent{},
	}
	journal := CommandJournal{Version: commandJournalVersion, Commands: map[string]*JournalCommand{commandID: journaled}}
	if err := store.SaveCommandJournal(journal); err != nil {
		t.Fatal(err)
	}
	daemon := &daemon{
		store: store, client: NewClient("https://control.example.test", "synthetic-credential", "1.2.3-test", nil),
		now: func() time.Time { return time.Date(2026, 9, 14, 10, 1, 0, 0, time.UTC) }, journal: journal,
		reportWake: make(chan struct{}, 1), active: map[string]context.CancelFunc{},
	}
	artifact := BackupArtifact{Database: "synthetic_accounts", Filename: "synthetic_accounts.sql", SnapshotID: strings.Repeat("b", 64)}

	daemon.databaseBackupProgress(commandID)(artifact, 1, 2)

	if journaled.Sequence != 2 || len(journaled.Events) != 1 || journaled.Events[0].Kind != "run_progress" {
		t.Fatalf("journaled progress: %+v", journaled)
	}
	if journaled.Events[0].Payload["databases_completed"] != 1 || journaled.Events[0].Payload["databases_total"] != 2 {
		t.Fatalf("progress payload: %+v", journaled.Events[0].Payload)
	}
	select {
	case <-daemon.reportWake:
	default:
		t.Fatal("database progress did not wake reporter")
	}
	persisted, err := store.LoadCommandJournal()
	if err != nil || len(persisted.Commands[commandID].Events) != 1 || persisted.Commands[commandID].Events[0].Kind != "run_progress" {
		t.Fatalf("persisted progress: %+v %v", persisted, err)
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
			Connection: testLocalRepository(filepath.Join(root, "repository")),
		},
	}
}
