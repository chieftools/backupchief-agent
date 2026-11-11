package agent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

const (
	// eventRejectionAttempts and eventRejectionWindow bound how long a durable event the
	// control plane permanently rejects is retried. Both must pass, so a control-plane
	// fix deployed within the window still receives the original result.
	eventRejectionAttempts = 3
	eventRejectionWindow   = 24 * time.Hour
)

var ErrEventsTooLarge = errors.New("events exceed the request limit")

type EventRejection struct {
	Attempts        int    `json:"attempts"`
	FirstRejectedAt string `json:"first_rejected_at"`
	Code            string `json:"code,omitempty"`
}

type eventSubmitter func(context.Context, []AgentEvent) (EventsResponse, error)

// submitEventsIsolated submits a batch and, when the control plane refuses the whole
// request as invalid, resubmits each event alone so one invalid event cannot block
// the others. It returns results for a prefix of the batch; a transient error stops
// the submission and is returned with the results gathered so far.
func submitEventsIsolated(ctx context.Context, submit eventSubmitter, batch []AgentEvent) ([]EventResult, error) {
	response, err := submit(ctx, batch)
	if err == nil {
		return response.Results, nil
	}
	code, permanent := permanentEventRequestRejection(err)
	if !permanent {
		return nil, err
	}
	if len(batch) == 1 {
		return []EventResult{{ID: batch[0].ID, Status: "rejected", Code: code}}, nil
	}

	results := make([]EventResult, 0, len(batch))
	for _, event := range batch {
		single, err := submitEventsIsolated(ctx, submit, []AgentEvent{event})
		results = append(results, single...)
		if err != nil {
			return results, err
		}
	}
	return results, nil
}

func permanentEventRequestRejection(err error) (string, bool) {
	if errors.Is(err, ErrEventsTooLarge) {
		return "payload_too_large", true
	}
	var apiError *APIError
	if errors.As(err, &apiError) && (apiError.Status == http.StatusUnprocessableEntity || apiError.Status == http.StatusRequestEntityTooLarge) {
		return apiError.Code, true
	}
	return "", false
}

func durableEventKind(kind string) bool {
	return kind == "run_finished" || kind == "snapshot_inventory_chunk" || kind == "replication_finished"
}

// applyEventResultsLocked removes delivered events from the command and decides what
// happens to rejected ones. A rejected durable event is retained until it has been
// rejected eventRejectionAttempts times over at least eventRejectionWindow, then it is
// quarantined: dropped together with the inventory chunks that belong to a dropped
// terminal result, and replaced by a reporting gap so the journal entry can drain.
// Other rejected events are discarded with a spool gap. The returned error reports a
// rejection that is still being retried. The caller holds daemon.mu.
func (daemon *daemon) applyEventResultsLocked(current *JournalCommand, batch []AgentEvent, results []EventResult) (bool, error) {
	now := time.Now().UTC()
	if daemon.now != nil {
		now = daemon.now()
	}
	retained := make([]AgentEvent, 0)
	dropped := 0
	droppedTerminal := false
	gap := false
	var rejection error

	for index, result := range results {
		event := batch[index]
		if result.Status != "rejected" {
			delete(current.EventRejections, event.ID)
			continue
		}
		if !durableEventKind(event.Kind) {
			gap = true
			continue
		}

		if current.EventRejections == nil {
			current.EventRejections = map[string]EventRejection{}
		}
		record := current.EventRejections[event.ID]
		record.Attempts++
		record.Code = result.Code
		if record.FirstRejectedAt == "" {
			record.FirstRejectedAt = protocolTimestamp(now)
		}
		firstRejectedAt, parseErr := time.Parse(time.RFC3339Nano, record.FirstRejectedAt)
		if parseErr == nil && record.Attempts >= eventRejectionAttempts && !now.Before(firstRejectedAt.Add(eventRejectionWindow)) {
			delete(current.EventRejections, event.ID)
			dropped++
			droppedTerminal = droppedTerminal || event.Kind == "run_finished"
			continue
		}
		current.EventRejections[event.ID] = record
		retained = append(retained, event)
		rejection = fmt.Errorf("event %s was rejected: %s", result.ID, result.Code)
	}

	pending := append(retained, current.Events[len(results):]...)
	if droppedTerminal {
		kept := pending[:0]
		for _, event := range pending {
			if event.Kind == "snapshot_inventory_chunk" {
				delete(current.EventRejections, event.ID)
				dropped++
				continue
			}
			kept = append(kept, event)
		}
		pending = kept
		rejection = nil
		for _, event := range pending {
			if record, exists := current.EventRejections[event.ID]; exists {
				rejection = fmt.Errorf("event %s was rejected: %s", event.ID, record.Code)
			}
		}
	}
	if dropped > 0 {
		current.Sequence++
		if eventID, err := newULID(now); err == nil {
			pending = append(pending, daemon.eventEnvelope(
				current, eventID, current.Sequence, protocolTimestamp(now), "reporting_gap",
				map[string]any{"reason": "event_rejected", "dropped_event_count": uint64(dropped), "dropped_log_bytes": uint64(0)},
			))
		}
		gap = true
	}
	if len(current.EventRejections) == 0 {
		current.EventRejections = nil
	}
	current.Events = pending
	if gap {
		daemon.markSpoolGapLocked()
	}
	return gap, rejection
}
