package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestReenrollAdvancesGenerationAndRetiresOldWork(t *testing.T) {
	store := newAgentTestStore(t)
	old := testBootstrap()
	oldTokenHash := old.SetupTokenHash
	newToken := "bcenr_syntheticReplacementToken1234567890ABCDE"
	now := time.Date(2026, 9, 11, 10, 30, 0, 0, time.UTC)
	var newCredential string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		switch request.URL.Path {
		case "/agent/v1/enroll":
			var enrollment EnrollmentRequest
			if err := json.NewDecoder(request.Body).Decode(&enrollment); err != nil {
				t.Errorf("decode enrollment: %v", err)
			}
			newCredential = enrollment.Credential
			response.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(response, `{"protocol_revision":"1.1.0","server_id":"01k4p4f7m1r9d3t6v8w2x5y7za","generation":2,"enrolled_at":"2026-09-11T10:30:00.000000Z","config_revision":1}`)
		case "/agent/v1/config":
			if request.Header.Get("Authorization") != "Bearer "+newCredential {
				t.Errorf("new config used the wrong credential")
			}
			body := testConfigBody(2, 1, ProtocolRevision)
			response.Header().Set("ETag", `"`+digestBody(body)+`"`)
			_, _ = response.Write(body)
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	old.Endpoint = server.URL + "/agent/v1"
	if err := store.SaveBootstrap(old); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SaveConfig(old, testConfigBody(1, 1, ProtocolRevision), nil); err != nil {
		t.Fatal(err)
	}
	receivedID := "01k4p4f7m1r9d3t6v8w2x5y7zb"
	runningID := "01k4p4f7m1r9d3t6v8w2x5y7zc"
	journal := CommandJournal{Version: commandJournalVersion, Commands: map[string]*JournalCommand{
		receivedID: retiredTestCommand(receivedID, "01k4p4f7m1r9d3t6v8w2x5y7zd", "received", now),
		runningID:  retiredTestCommand(runningID, "01k4p4f7m1r9d3t6v8w2x5y7ze", "running", now),
	}}
	if err := store.SaveCommandJournal(journal); err != nil {
		t.Fatal(err)
	}
	manager := &fakeServiceManager{}
	options := testEnrollOptions(store, server, manager)
	options.Now = func() time.Time { return now }

	bootstrap, err := Enroll(context.Background(), newToken, options)
	if err != nil {
		t.Fatal(err)
	}
	if bootstrap.ServerID != old.ServerID || bootstrap.Generation != 2 || bootstrap.SetupTokenHash == oldTokenHash {
		t.Fatalf("replacement identity: %+v", bootstrap)
	}
	if manager.stopCount() != 1 || manager.callCount() != 1 {
		t.Fatalf("service transitions: stop=%d start=%d", manager.stopCount(), manager.callCount())
	}
	persisted, err := store.LoadCommandJournal()
	if err != nil {
		t.Fatal(err)
	}
	for id, command := range persisted.Commands {
		if command.State != "finished" || command.JobSnapshot != "" || !command.ResultReported || len(command.Events) != 1 || command.Events[0].Kind != "run_finished" {
			t.Fatalf("retired command %s: %+v", id, command)
		}
		if command.RunKind != "backup" || command.Events[0].RunKind != "backup" || command.Result.RunKind != "backup" {
			t.Fatalf("retired command %s: %+v", id, command)
		}
	}
	if persisted.Commands[receivedID].Result.ResultCode != "cancelled" || persisted.Commands[runningID].Result.ResultCode != "outcome_unresolved" {
		t.Fatalf("retired results: %+v", persisted.Commands)
	}
	records, err := store.loadRetiredEnrollments("")
	if err != nil || len(records) != 1 || records[0].Credential != old.Credential || records[0].Generation != 1 {
		t.Fatalf("retired enrollment: %+v %v", records, err)
	}
	state, err := store.LoadRuntimeState()
	if err != nil || !reflect.DeepEqual(state, RuntimeState{}) {
		t.Fatalf("runtime state was not reset: %+v %v", state, err)
	}
}

func TestReenrollReplaysIdentityAfterResponseLoss(t *testing.T) {
	store := newAgentTestStore(t)
	old := testBootstrap()
	token := "bcenr_syntheticReenrollmentReplay123456789ABCDE"
	now := time.Date(2026, 9, 11, 11, 30, 0, 0, time.UTC)
	var requests []EnrollmentRequest
	var credential string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		switch request.URL.Path {
		case "/agent/v1/enroll":
			var enrollment EnrollmentRequest
			if err := json.NewDecoder(request.Body).Decode(&enrollment); err != nil {
				t.Errorf("decode enrollment: %v", err)
			}
			requests = append(requests, enrollment)
			credential = enrollment.Credential
			response.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(response, `{"protocol_revision":"1.1.0","server_id":"01k4p4f7m1r9d3t6v8w2x5y7za","generation":2,"enrolled_at":"2026-09-11T11:30:00.000000Z","config_revision":1}`)
		case "/agent/v1/config":
			if request.Header.Get("Authorization") != "Bearer "+credential {
				t.Errorf("new config used the wrong credential")
			}
			body := testConfigBody(2, 1, ProtocolRevision)
			response.Header().Set("ETag", `"`+digestBody(body)+`"`)
			_, _ = response.Write(body)
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	old.Endpoint = server.URL + "/agent/v1"
	if err := store.SaveBootstrap(old); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SaveConfig(old, testConfigBody(1, 1, ProtocolRevision), nil); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCommandJournal(CommandJournal{Version: commandJournalVersion, Commands: map[string]*JournalCommand{}}); err != nil {
		t.Fatal(err)
	}
	manager := &fakeServiceManager{}
	options := testEnrollOptions(store, server, manager)
	options.Now = func() time.Time { return now }
	losingClient := *server.Client()
	losingClient.Transport = &loseFirstEnrollmentResponse{base: losingClient.Transport}
	options.HTTPClient = &losingClient

	if _, err := Enroll(context.Background(), token, options); err == nil {
		t.Fatal("expected the lost re-enrollment response to fail")
	}
	_, err := store.LoadPending()
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.loadRetiredEnrollments("")
	if err != nil || len(before) != 1 {
		t.Fatalf("retired identity after response loss: %+v %v", before, err)
	}
	options.HTTPClient = server.Client()
	bootstrap, err := Enroll(context.Background(), token, options)
	if err != nil {
		t.Fatal(err)
	}
	after, err := store.loadRetiredEnrollments("")
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 2 || requests[0].AttemptID != requests[1].AttemptID || requests[0].Credential != requests[1].Credential {
		t.Fatalf("re-enrollment replay changed identity: %+v", requests)
	}
	if bootstrap.Generation != 2 || manager.stopCount() != 2 || manager.callCount() != 1 {
		t.Fatalf("re-enrollment resume: bootstrap=%+v stop=%d start=%d", bootstrap, manager.stopCount(), manager.callCount())
	}
	if len(after) != 1 || after[0].ExpiresAt != before[0].ExpiresAt {
		t.Fatalf("retired grace changed during replay: before=%+v after=%+v", before, after)
	}
}

func TestRetiredReporterUsesOnlyTerminalEndpoints(t *testing.T) {
	store := newAgentTestStore(t)
	current := testBootstrap()
	current.Generation = 2
	oldCredential := testBootstrap().Credential
	commandID := "01k4p4f7m1r9d3t6v8w2x5y7zb"
	runID := "01k4p4f7m1r9d3t6v8w2x5y7zc"
	logID := "01k4p4f7m1r9d3t6v8w2x5y7zd"
	command := retiredTestCommand(commandID, runID, "finished", time.Now())
	command.Result = &CommandResult{Generation: 1, RunID: runID, JobID: command.Command.Payload.JobID, Status: "cancelled", ResultCode: "cancelled", SnapshotIDs: []string{}}
	command.ResultReported = true
	command.Events = []AgentEvent{{
		ID: "01k4p4f7m1r9d3t6v8w2x5y7ze", RunID: runID, JobID: command.Command.Payload.JobID,
		ConfigRevision: 1, Trigger: "manual", Sequence: 1, OccurredAt: protocolTimestamp(time.Now()),
		Kind: "run_finished", Payload: terminalEventPayload(*command.Result),
	}}
	log := []byte("synthetic retired generation log")
	if err := store.WriteRunLog(logID, log); err != nil {
		t.Fatal(err)
	}
	command.LogID = logID
	command.LogBytes = len(log)
	command.LogSHA256 = digestBody(log)
	journal := CommandJournal{Version: commandJournalVersion, Commands: map[string]*JournalCommand{commandID: command}}
	if err := store.SaveCommandJournal(journal); err != nil {
		t.Fatal(err)
	}
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		if request.Header.Get("Authorization") != "Bearer "+oldCredential {
			t.Errorf("retired request used the wrong credential")
		}
		paths = append(paths, request.URL.Path)
		switch {
		case request.URL.Path == "/agent/v1/events":
			_, _ = io.WriteString(response, `{"protocol_revision":"1.1.0","results":[{"id":"01k4p4f7m1r9d3t6v8w2x5y7ze","status":"accepted"}]}`)
		case request.Method == http.MethodPut:
			response.WriteHeader(http.StatusNoContent)
		case request.URL.Path == "/agent/v1/runs/"+runID+"/logs/"+logID+"/complete":
			response.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("retired reporter used forbidden endpoint %s", request.URL.Path)
			response.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	if err := store.saveRetiredEnrollments("", []retiredEnrollment{{
		Endpoint: server.URL + "/agent/v1", ServerID: current.ServerID, Generation: 1,
		Credential: oldCredential, ExpiresAt: protocolTimestamp(time.Now().Add(time.Hour)),
	}}); err != nil {
		t.Fatal(err)
	}
	runtime := &daemon{
		store: store, bootstrap: current, now: time.Now,
		client:  NewClient(server.URL+"/agent/v1", current.Credential, "1.1.0", server.Client()),
		journal: journal, active: map[string]context.CancelFunc{}, repositories: map[string]bool{},
	}

	if err := runtime.reportRetiredJournal(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 3 {
		t.Fatalf("terminal request paths: %v", paths)
	}
	persisted, err := store.LoadCommandJournal()
	if err != nil || len(persisted.Commands) != 0 {
		t.Fatalf("retired journal remains: %+v %v", persisted, err)
	}
	records, err := store.loadRetiredEnrollments("")
	if err != nil || len(records) != 0 {
		t.Fatalf("retired identity remains: %+v %v", records, err)
	}
}

func TestRetiredReporterDropsOnlyTheUnknownRun(t *testing.T) {
	store := newAgentTestStore(t)
	current := testBootstrap()
	current.Generation = 2
	firstID := "01k4p4f7m1r9d3t6v8w2x5y7zb"
	secondID := "01k4p4f7m1r9d3t6v8w2x5y7zc"
	first := retiredTestCommand(firstID, "01k4p4f7m1r9d3t6v8w2x5y7zd", "finished", time.Now())
	second := retiredTestCommand(secondID, "01k4p4f7m1r9d3t6v8w2x5y7ze", "finished", time.Now())
	first.Events = []AgentEvent{{
		ID: "01k4p4f7m1r9d3t6v8w2x5y7zf", RunID: first.RunID, JobID: first.Command.Payload.JobID,
		ConfigRevision: 1, Trigger: "manual", Sequence: 1, OccurredAt: protocolTimestamp(time.Now()),
		Kind: "run_finished", Payload: map[string]any{},
	}}
	second.Events = []AgentEvent{{
		ID: "01k4p4f7m1r9d3t6v8w2x5y7zg", RunID: second.RunID, JobID: second.Command.Payload.JobID,
		ConfigRevision: 1, Trigger: "manual", Sequence: 1, OccurredAt: protocolTimestamp(time.Now()),
		Kind: "run_finished", Payload: map[string]any{},
	}}
	journal := CommandJournal{Version: commandJournalVersion, Commands: map[string]*JournalCommand{
		firstID: first, secondID: second,
	}}
	if err := store.SaveCommandJournal(journal); err != nil {
		t.Fatal(err)
	}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		requests++
		if requests == 1 {
			response.WriteHeader(http.StatusGone)
			_, _ = io.WriteString(response, `{"type":"about:blank","title":"Gone","status":410,"code":"enrollment_revoked","detail":"The retired run is unknown."}`)
			return
		}
		_, _ = io.WriteString(response, `{"protocol_revision":"1.1.0","results":[{"id":"01k4p4f7m1r9d3t6v8w2x5y7zg","status":"accepted"}]}`)
	}))
	defer server.Close()
	if err := store.saveRetiredEnrollments("", []retiredEnrollment{{
		Endpoint: server.URL + "/agent/v1", ServerID: current.ServerID, Generation: 1,
		Credential: testBootstrap().Credential, ExpiresAt: protocolTimestamp(time.Now().Add(time.Hour)),
	}}); err != nil {
		t.Fatal(err)
	}
	runtime := &daemon{
		store: store, bootstrap: current, now: time.Now,
		client:  NewClient(server.URL+"/agent/v1", current.Credential, "1.1.0", server.Client()),
		journal: journal, active: map[string]context.CancelFunc{}, repositories: map[string]bool{},
	}

	if err := runtime.reportRetiredJournal(context.Background()); err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("terminal requests: %d", requests)
	}
	persisted, err := store.LoadCommandJournal()
	if err != nil || len(persisted.Commands) != 0 {
		t.Fatalf("retired journal remains: %+v %v", persisted, err)
	}
}

func TestAuthenticationPauseBlocksDispatchAndConfigOmissionSkipsQueuedWork(t *testing.T) {
	store := newAgentTestStore(t)
	bootstrap := testBootstrap()
	if _, err := store.SaveConfig(bootstrap, testConfigBody(1, 2, ProtocolRevision), nil); err != nil {
		t.Fatal(err)
	}
	commandID := "01k4p4f7m1r9d3t6v8w2x5y7zb"
	command := retiredTestCommand(commandID, "01k4p4f7m1r9d3t6v8w2x5y7zc", "received", time.Now())
	command.Acknowledged = true
	command.JobSnapshot = ""
	executor := &recordingExecutor{}
	runtime := &daemon{
		store: store, bootstrap: bootstrap, now: time.Now,
		metadata: ConfigMetadata{Generation: 1, Revision: 2},
		state:    RuntimeState{AuthenticationPaused: true},
		journal:  CommandJournal{Version: commandJournalVersion, Commands: map[string]*JournalCommand{commandID: command}},
		executor: executor, active: map[string]context.CancelFunc{}, repositories: map[string]bool{},
	}
	if err := runtime.dispatchCommands(context.Background()); err != nil {
		t.Fatal(err)
	}
	if command.State != "received" || len(executor.requests) != 0 {
		t.Fatalf("paused dispatcher changed work: %+v", command)
	}
	runtime.state.AuthenticationPaused = false
	if err := runtime.advanceCommands(context.Background()); err != nil {
		t.Fatal(err)
	}
	if command.Result == nil || command.Result.ResultCode != "config_unavailable" || len(executor.requests) != 0 {
		t.Fatalf("omitted job was executed: %+v requests=%d", command, len(executor.requests))
	}
}

func TestRevocationCancelsActiveRunBeforeReturningPermanentStop(t *testing.T) {
	store := newAgentTestStore(t)
	job := executionJob(t.TempDir())
	commandID := "01k4p4f7m1r9d3t6v8w2x5y7zb"
	command := retiredTestCommand(commandID, "01k4p4f7m1r9d3t6v8w2x5y7zc", "received", time.Now())
	command.Acknowledged = true
	executor := &blockingExecutor{started: make(chan struct{}), stopped: make(chan struct{})}
	runtime := &daemon{
		store: store, bootstrap: testBootstrap(), now: time.Now,
		journal:  CommandJournal{Version: commandJournalVersion, Commands: map[string]*JournalCommand{commandID: command}},
		executor: executor, active: map[string]context.CancelFunc{}, repositories: map[string]bool{},
	}
	if err := runtime.startBackup(context.Background(), commandID, job); err != nil {
		t.Fatal(err)
	}
	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("backup did not start")
	}
	err := runtime.handleRequestError(&APIError{Status: http.StatusGone, Code: "enrollment_revoked"})
	if !errors.Is(err, ErrPermanentlyStopped) {
		t.Fatalf("revocation result: %v", err)
	}
	runtime.activeWG.Wait()
	select {
	case <-executor.stopped:
	default:
		t.Fatal("revocation returned before the executor stopped")
	}
	persisted, err := store.LoadCommandJournal()
	if err != nil || persisted.Commands[commandID].Result == nil || persisted.Commands[commandID].Result.ResultCode != "cancelled" {
		t.Fatalf("terminal result was not durable: %+v %v", persisted, err)
	}
}

func retiredTestCommand(commandID, runID, state string, now time.Time) *JournalCommand {
	return &JournalCommand{
		Command: AgentCommand{
			ID: commandID, Generation: 1, Kind: "run_backup",
			IssuedAt: protocolTimestamp(now.Add(-time.Minute)), ExpiresAt: protocolTimestamp(now.Add(time.Hour)),
			Payload: CommandPayload{JobID: "01k4p4f7m1r9d3t6v8w2x5y7zf", RequiredConfigRevision: 1},
		},
		RunID: runID, ConfigRevision: 1, Trigger: "manual", JobSnapshot: "synthetic-encoded-job",
		ReceivedAt: protocolTimestamp(now), Acknowledged: true, State: state, Events: []AgentEvent{},
	}
}
