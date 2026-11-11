package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

const (
	deliveryCommandID  = "01k4p4f7m1r9d3t6v8w2x5y7zb"
	deliveryRunID      = "01k4p4f7m1r9d3t6v8w2x5y7zc"
	deliveryTerminalID = "01k4p4f7m1r9d3t6v8w2x5y7zd"
	deliveryChunkID    = "01k4p4f7m1r9d3t6v8w2x5y7ze"
	deliveryProgressID = "01k4p4f7m1r9d3t6v8w2x5y7zf"
)

type eventDeliveryServer struct {
	mu       sync.Mutex
	requests [][]string
	respond  func(response http.ResponseWriter, ids []string)
}

func newEventDeliveryServer(t *testing.T, respond func(response http.ResponseWriter, ids []string)) (*eventDeliveryServer, *httptest.Server) {
	t.Helper()
	recorder := &eventDeliveryServer{respond: respond}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var body EventRequest
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode events: %v", err)
		}
		ids := make([]string, 0, len(body.Events))
		for _, event := range body.Events {
			ids = append(ids, event.ID)
		}
		recorder.mu.Lock()
		recorder.requests = append(recorder.requests, ids)
		recorder.mu.Unlock()
		respond(response, ids)
	}))
	t.Cleanup(server.Close)
	return recorder, server
}

func writeEventResults(response http.ResponseWriter, ids []string, status func(string) EventResult) {
	results := make([]EventResult, 0, len(ids))
	for _, id := range ids {
		results = append(results, status(id))
	}
	response.Header().Set(ProtocolHeader, ProtocolRevision)
	_ = json.NewEncoder(response).Encode(EventsResponse{ProtocolRevision: ProtocolRevision, Results: results})
}

func deliveryDaemon(t *testing.T, server *httptest.Server, now time.Time, events []AgentEvent) *daemon {
	t.Helper()
	journal := newCommandJournal()
	journal.Commands[deliveryCommandID] = &JournalCommand{
		Command:        AgentCommand{ID: deliveryCommandID, Generation: 1, Kind: "scheduled_forget"},
		RunID:          deliveryRunID,
		RunKind:        "forget",
		Trigger:        "scheduled",
		Acknowledged:   true,
		ResultReported: true,
		Sequence:       uint64(len(events)),
		Events:         events,
	}
	return &daemon{
		store:     newAgentTestStore(t),
		client:    NewClient(server.URL, "synthetic-credential", ProtocolRevision, server.Client()),
		bootstrap: Bootstrap{Generation: 1},
		journal:   journal,
		now:       func() time.Time { return now },
	}
}

func TestInvalidEventBatchIsResubmittedEventByEventSoValidEventsAreDelivered(t *testing.T) {
	recorder, server := newEventDeliveryServer(t, func(response http.ResponseWriter, ids []string) {
		for _, id := range ids {
			if id == deliveryTerminalID {
				writeProblem(response, http.StatusUnprocessableEntity, "validation_failed")
				return
			}
		}
		writeEventResults(response, ids, func(id string) EventResult { return EventResult{ID: id, Status: "accepted"} })
	})
	runtime := deliveryDaemon(t, server, time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC), []AgentEvent{
		{ID: deliveryProgressID, Kind: "run_progress"},
		{ID: deliveryTerminalID, Kind: "run_finished"},
	})

	if err := runtime.flushCommand(context.Background(), deliveryCommandID); err == nil {
		t.Fatal("the rejected terminal event did not return an error")
	}

	command := runtime.journal.Commands[deliveryCommandID]
	if len(command.Events) != 1 || command.Events[0].ID != deliveryTerminalID {
		t.Fatalf("pending events: %+v", command.Events)
	}
	if record := command.EventRejections[deliveryTerminalID]; record.Attempts != 1 || record.Code != "validation_failed" {
		t.Fatalf("terminal rejection record: %+v", record)
	}
	if len(recorder.requests) != 3 || len(recorder.requests[1]) != 1 || recorder.requests[1][0] != deliveryProgressID {
		t.Fatalf("submitted batches: %v", recorder.requests)
	}
	if runtime.state.SpoolGapDetected {
		t.Fatal("a retained terminal rejection claimed evidence was lost")
	}
}

func TestPermanentlyRejectedTerminalIsQuarantinedAfterTheRetryWindowAndTheCommandDrains(t *testing.T) {
	now := time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)
	_, server := newEventDeliveryServer(t, func(response http.ResponseWriter, ids []string) {
		writeEventResults(response, ids, func(id string) EventResult {
			if id == deliveryTerminalID || id == deliveryChunkID {
				return EventResult{ID: id, Status: "rejected", Code: "identity_mismatch"}
			}
			return EventResult{ID: id, Status: "accepted"}
		})
	})
	runtime := deliveryDaemon(t, server, now, []AgentEvent{
		{ID: deliveryChunkID, Kind: "snapshot_inventory_chunk", Sequence: 1},
		{ID: deliveryTerminalID, Kind: "run_finished", Sequence: 2},
	})
	runtime.journal.Commands[deliveryCommandID].EventRejections = map[string]EventRejection{
		deliveryTerminalID: {Attempts: eventRejectionAttempts - 1, FirstRejectedAt: protocolTimestamp(now.Add(-eventRejectionWindow))},
	}

	if err := runtime.flushCommand(context.Background(), deliveryCommandID); err != nil {
		t.Fatalf("quarantine flush: %v", err)
	}

	command := runtime.journal.Commands[deliveryCommandID]
	if len(command.Events) != 1 || command.Events[0].Kind != "reporting_gap" {
		t.Fatalf("pending events after quarantine: %+v", command.Events)
	}
	gap := command.Events[0]
	if gap.Sequence != 3 || gap.Payload["reason"] != "event_rejected" || gap.Payload["dropped_event_count"] != uint64(2) {
		t.Fatalf("reporting gap: %+v", gap)
	}
	if command.EventRejections != nil || !runtime.state.SpoolGapDetected {
		t.Fatalf("quarantine state: rejections=%+v gap=%t", command.EventRejections, runtime.state.SpoolGapDetected)
	}
	persisted, err := runtime.store.LoadRuntimeState()
	if err != nil || !persisted.SpoolGapDetected {
		t.Fatalf("persisted gap state: %+v %v", persisted, err)
	}

	if err := runtime.flushCommand(context.Background(), deliveryCommandID); err != nil {
		t.Fatalf("gap flush: %v", err)
	}
	if _, exists := runtime.journal.Commands[deliveryCommandID]; exists {
		t.Fatal("the quarantined command did not drain from the journal")
	}
}

func TestRejectedTerminalIsRetainedUntilTheRetryWindowPassesEvenAfterTheAttemptLimit(t *testing.T) {
	now := time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)
	_, server := newEventDeliveryServer(t, func(response http.ResponseWriter, ids []string) {
		writeEventResults(response, ids, func(id string) EventResult {
			return EventResult{ID: id, Status: "rejected", Code: "identity_mismatch"}
		})
	})
	runtime := deliveryDaemon(t, server, now, []AgentEvent{{ID: deliveryTerminalID, Kind: "run_finished"}})
	runtime.journal.Commands[deliveryCommandID].EventRejections = map[string]EventRejection{
		deliveryTerminalID: {Attempts: eventRejectionAttempts + 5, FirstRejectedAt: protocolTimestamp(now.Add(-eventRejectionWindow + time.Minute))},
	}

	if err := runtime.flushCommand(context.Background(), deliveryCommandID); err == nil {
		t.Fatal("the retained terminal rejection did not return an error")
	}
	command := runtime.journal.Commands[deliveryCommandID]
	if len(command.Events) != 1 || command.Events[0].ID != deliveryTerminalID || runtime.state.SpoolGapDetected {
		t.Fatalf("terminal was quarantined inside the retry window: %+v", command.Events)
	}
}

func TestTransientEventFailureKeepsEventsWithoutCountingARejection(t *testing.T) {
	_, server := newEventDeliveryServer(t, func(response http.ResponseWriter, _ []string) {
		writeProblem(response, http.StatusServiceUnavailable, "temporarily_unavailable")
	})
	runtime := deliveryDaemon(t, server, time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC), []AgentEvent{
		{ID: deliveryChunkID, Kind: "snapshot_inventory_chunk"},
		{ID: deliveryTerminalID, Kind: "run_finished"},
	})

	if err := runtime.flushCommand(context.Background(), deliveryCommandID); err == nil {
		t.Fatal("a transient failure did not return an error")
	}
	command := runtime.journal.Commands[deliveryCommandID]
	if len(command.Events) != 2 || command.EventRejections != nil {
		t.Fatalf("transient failure changed pending events: events=%+v rejections=%+v", command.Events, command.EventRejections)
	}
}

func TestOversizedEventRequestIsIsolatedAndTheOversizedEventIsRejected(t *testing.T) {
	submit := func(_ context.Context, events []AgentEvent) (EventsResponse, error) {
		for _, event := range events {
			if event.ID == deliveryTerminalID {
				return EventsResponse{}, ErrEventsTooLarge
			}
		}
		results := make([]EventResult, 0, len(events))
		for _, event := range events {
			results = append(results, EventResult{ID: event.ID, Status: "accepted"})
		}
		return EventsResponse{Results: results}, nil
	}

	results, err := submitEventsIsolated(context.Background(), submit, []AgentEvent{
		{ID: deliveryChunkID, Kind: "snapshot_inventory_chunk"},
		{ID: deliveryTerminalID, Kind: "run_finished"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].Status != "accepted" || results[1].Status != "rejected" || results[1].Code != "payload_too_large" {
		t.Fatalf("isolated results: %+v", results)
	}
}
