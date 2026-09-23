package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCommandDispatchContinuesAfterEarlierScheduledAcknowledgementFails(t *testing.T) {
	staleID := "01k4p4f7m1r9d3t6v8w2x5y7aa"
	freshID := "01k4p4f7m1r9d3t6v8w2x5y7bb"
	runtime, acknowledgements, closeServer := dispatchTestDaemon(t, staleID, freshID, true)
	defer closeServer()

	stale := runtime.journal.Commands[staleID]
	stale.Command.Kind = "scheduled_backup"
	stale.Trigger = "scheduled"
	stale.JobSnapshot = "invalid-snapshot"

	if err := runtime.advanceCommands(context.Background()); err == nil || !strings.Contains(err.Error(), staleID) {
		t.Fatalf("expected contextual stale command error, got %v", err)
	}
	runtime.activeWG.Wait()
	if stale.Acknowledged || stale.State != "received" {
		t.Fatalf("failed command was not retained for retry: %+v", stale)
	}
	fresh := runtime.journal.Commands[freshID]
	if !fresh.Acknowledged || fresh.State == "received" {
		t.Fatalf("fresh backup did not advance: %+v", fresh)
	}
	acknowledgements.mu.Lock()
	if acknowledgements.counts[freshID] != 1 {
		t.Errorf("fresh acknowledgement count: %d", acknowledgements.counts[freshID])
	}
	acknowledgements.mu.Unlock()

	if err := runtime.advanceCommands(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !stale.Acknowledged || stale.Result == nil || stale.Result.ResultCode != "config_unavailable" {
		t.Fatalf("invalid saved scheduled job was not skipped: %+v", stale)
	}
	acknowledgements.mu.Lock()
	defer acknowledgements.mu.Unlock()
	if acknowledgements.counts[freshID] != 1 || acknowledgements.counts[staleID] != 2 {
		t.Fatalf("acknowledgement counts: %+v", acknowledgements.counts)
	}
}

func TestCommandDispatchSkipsInvalidScheduledSnapshotBeforeFreshBackup(t *testing.T) {
	staleID := "01k4p4f7m1r9d3t6v8w2x5y7aa"
	freshID := "01k4p4f7m1r9d3t6v8w2x5y7bb"
	runtime, acknowledgements, closeServer := dispatchTestDaemon(t, staleID, freshID, false)
	defer closeServer()
	stale := runtime.journal.Commands[staleID]
	stale.Command.Kind = "scheduled_backup"
	stale.Trigger = "scheduled"
	stale.Acknowledged = true
	stale.JobSnapshot = "invalid-snapshot"

	if err := runtime.advanceCommands(context.Background()); err != nil {
		t.Fatal(err)
	}
	runtime.activeWG.Wait()
	if stale.Result == nil || stale.Result.ResultCode != "config_unavailable" {
		t.Fatalf("invalid scheduled snapshot was not skipped: %+v", stale)
	}
	if fresh := runtime.journal.Commands[freshID]; !fresh.Acknowledged || fresh.State == "received" {
		t.Fatalf("fresh backup did not advance: %+v", fresh)
	}
	acknowledgements.mu.Lock()
	defer acknowledgements.mu.Unlock()
	if acknowledgements.counts[staleID] != 0 || acknowledgements.counts[freshID] != 1 {
		t.Fatalf("acknowledgement counts: %+v", acknowledgements.counts)
	}
}

func TestCommandDispatchDiscardsExpiredManualEntryAndAcknowledgesFreshBackup(t *testing.T) {
	staleID := "01k4p4f7m1r9d3t6v8w2x5y7aa"
	freshID := "01k4p4f7m1r9d3t6v8w2x5y7bb"
	runtime, acknowledgements, closeServer := dispatchTestDaemon(t, staleID, freshID, false)
	defer closeServer()
	runtime.journal.Commands[staleID].Command.ExpiresAt = protocolTimestamp(runtime.now().Add(-time.Minute))

	if err := runtime.advanceCommands(context.Background()); err != nil {
		t.Fatal(err)
	}
	runtime.activeWG.Wait()
	if runtime.journal.Commands[staleID] != nil {
		t.Fatal("expired manual entry remains in journal")
	}
	if fresh := runtime.journal.Commands[freshID]; !fresh.Acknowledged || fresh.State == "received" {
		t.Fatalf("fresh backup did not advance: %+v", fresh)
	}
	acknowledgements.mu.Lock()
	defer acknowledgements.mu.Unlock()
	if acknowledgements.counts[staleID] != 0 || acknowledgements.counts[freshID] != 1 {
		t.Fatalf("acknowledgement counts: %+v", acknowledgements.counts)
	}
}

type dispatchAcknowledgements struct {
	mu     sync.Mutex
	counts map[string]int
}

func dispatchTestDaemon(t *testing.T, staleID, freshID string, failStaleOnce bool) (*daemon, *dispatchAcknowledgements, func()) {
	t.Helper()
	now := time.Date(2026, 9, 9, 8, 15, 0, 0, time.UTC)
	acknowledgements := &dispatchAcknowledgements{counts: map[string]int{}}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		if !strings.HasSuffix(request.URL.Path, "/ack") {
			http.NotFound(response, request)
			return
		}
		var acknowledgement CommandAcknowledgement
		if err := json.NewDecoder(request.Body).Decode(&acknowledgement); err != nil || !ulidPattern.MatchString(acknowledgement.RunID) {
			t.Errorf("invalid acknowledgement: %+v %v", acknowledgement, err)
		}
		commandID := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, "/agent/v1/commands/"), "/ack")
		acknowledgements.mu.Lock()
		acknowledgements.counts[commandID]++
		count := acknowledgements.counts[commandID]
		acknowledgements.mu.Unlock()
		if failStaleOnce && commandID == staleID && count == 1 {
			response.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(response).Encode(map[string]any{"code": "temporarily_unavailable", "status": http.StatusServiceUnavailable})
			return
		}
		response.WriteHeader(http.StatusNoContent)
	}))
	store := newAgentTestStore(t)
	bootstrap := testBootstrap()
	bootstrap.Endpoint = server.URL + "/agent/v1"
	jobID := "01k4p4f7m1r9d3t6v8w2x5y7zc"
	config := runtimeConfig(t.TempDir(), jobID)
	body, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SaveConfig(bootstrap, body, nil); err != nil {
		t.Fatal(err)
	}
	stale := retiredTestCommand(staleID, "01k4p4f7m1r9d3t6v8w2x5y7ac", "received", now)
	fresh := retiredTestCommand(freshID, "01k4p4f7m1r9d3t6v8w2x5y7bc", "received", now)
	for _, command := range []*JournalCommand{stale, fresh} {
		command.Acknowledged = false
		command.JobSnapshot = ""
		command.Command.Payload.JobID = jobID
		command.Command.Payload.RequiredConfigRevision = 2
	}
	journal := CommandJournal{Version: commandJournalVersion, Commands: map[string]*JournalCommand{staleID: stale, freshID: fresh}}
	if err := store.SaveCommandJournal(journal); err != nil {
		t.Fatal(err)
	}
	acknowledged := func() bool {
		acknowledgements.mu.Lock()
		count := acknowledgements.counts[freshID]
		acknowledgements.mu.Unlock()
		persisted, err := store.LoadCommandJournal()
		return err == nil && count == 1 && persisted.Commands[freshID] != nil && persisted.Commands[freshID].Acknowledged
	}
	runtime := &daemon{
		store: store, client: NewClient(bootstrap.Endpoint, bootstrap.Credential, "1.1.0", server.Client()),
		bootstrap: bootstrap, now: func() time.Time { return now }, metadata: ConfigMetadata{Generation: 1, Revision: 2},
		journal: journal, executor: &flowExecutor{t: t, acked: acknowledged},
		active: map[string]context.CancelFunc{}, repositories: map[string]bool{},
	}
	return runtime, acknowledgements, server.Close
}
