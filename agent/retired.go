package agent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"
)

var retiredEnrollmentsKey = []byte("retired_enrollments")

type retiredEnrollment struct {
	Endpoint   string `json:"endpoint"`
	ServerID   string `json:"server_id"`
	Generation uint64 `json:"generation"`
	Credential string `json:"credential"`
	ExpiresAt  string `json:"expires_at"`
}

func (store *FileStore) rotateRetiredEnrollments(oldKey, newKey string, retired retiredEnrollment) error {
	records, oldErr := store.loadRetiredEnrollments(oldKey)
	if oldErr != nil {
		records, oldErr = store.loadRetiredEnrollments(newKey)
	}
	if oldErr != nil {
		return oldErr
	}
	found := false
	for index := range records {
		if records[index].ServerID == retired.ServerID && records[index].Generation == retired.Generation {
			retired.ExpiresAt = records[index].ExpiresAt
			records[index] = retired
			found = true
		}
	}
	if !found {
		records = append(records, retired)
	}
	return store.saveRetiredEnrollments(newKey, records)
}

func (store *FileStore) loadRetiredEnrollments(cacheKey string) ([]retiredEnrollment, error) {
	store.stateMu.Lock()
	defer store.stateMu.Unlock()

	var encoded []byte
	err := store.viewState(func(transaction *bolt.Tx) error {
		value := transaction.Bucket(metadataBucket).Get(retiredEnrollmentsKey)
		if value != nil {
			encoded = append([]byte(nil), value...)
		}
		return nil
	})
	if err != nil || encoded == nil {
		return nil, err
	}
	return openRetiredEnrollments(cacheKey, encoded)
}

func (store *FileStore) saveRetiredEnrollments(cacheKey string, records []retiredEnrollment) error {
	store.stateMu.Lock()
	defer store.stateMu.Unlock()

	return store.saveRetiredEnrollmentsLocked(cacheKey, records)
}

func (store *FileStore) saveRetiredEnrollmentsLocked(cacheKey string, records []retiredEnrollment) error {
	return store.updateState(func(transaction *bolt.Tx) error {
		bucket := transaction.Bucket(metadataBucket)
		if len(records) == 0 {
			return bucket.Delete(retiredEnrollmentsKey)
		}
		encoded, err := sealRetiredEnrollments(cacheKey, records)
		if err != nil {
			return err
		}
		return bucket.Put(retiredEnrollmentsKey, encoded)
	})
}

func sealRetiredEnrollments(cacheKey string, records []retiredEnrollment) ([]byte, error) {
	encoded, err := json.Marshal(records)
	if err != nil {
		return nil, fmt.Errorf("encode retired enrollments: %w", err)
	}
	return encoded, nil
}

func openRetiredEnrollments(cacheKey string, encoded []byte) ([]retiredEnrollment, error) {
	var records []retiredEnrollment
	if err := json.Unmarshal(encoded, &records); err != nil {
		return nil, fmt.Errorf("decode retired enrollments: %w", err)
	}
	for _, record := range records {
		if !validControlPlaneEndpoint(record.Endpoint) || !ulidPattern.MatchString(record.ServerID) || record.Generation == 0 || !validateTimestamp(record.ExpiresAt) {
			return nil, fmt.Errorf("retired enrollment is invalid")
		}
		credential, err := base64.RawURLEncoding.DecodeString(record.Credential)
		if err != nil || len(credential) < 32 {
			return nil, fmt.Errorf("retired enrollment credential is invalid")
		}
	}
	return records, nil
}

func prepareRetiredGeneration(store *FileStore, bootstrap Bootstrap, now time.Time) error {
	journal, err := store.LoadCommandJournal()
	if err != nil {
		return err
	}
	for _, command := range journal.Commands {
		if command.Command.Generation != bootstrap.Generation {
			continue
		}
		if command.State == "received" || command.State == "running" {
			status, resultCode, summary := "cancelled", "cancelled", "The backup was cancelled before re-enrollment."
			if command.State == "running" {
				status, resultCode, summary = "unresolved", "outcome_unresolved", "Re-enrollment ended the run before its outcome was confirmed."
			}
			timestamp := protocolTimestamp(now)
			command.State = "finished"
			command.Result = &CommandResult{
				Generation: bootstrap.Generation, RunID: command.RunID, JobID: command.Command.Payload.JobID,
				RunKind: command.RunKind, Status: status, ResultCode: resultCode, StartedAt: command.ReceivedAt, FinishedAt: timestamp,
				SnapshotIDs: []string{}, Summary: summary,
			}
			command.Sequence++
			eventID, idErr := newULID(now)
			if idErr != nil {
				return idErr
			}
			command.Events = append(command.Events, AgentEvent{
				ID: eventID, RunID: command.RunID, JobID: command.Command.Payload.JobID,
				RunKind: command.RunKind, ConfigRevision: command.ConfigRevision, Trigger: command.Trigger, ScheduledFor: command.ScheduledFor,
				Sequence: command.Sequence, OccurredAt: timestamp, Kind: "run_finished",
				Payload: terminalEventPayload(*command.Result),
			})
		}
		terminalEvents := command.Events[:0]
		for _, event := range command.Events {
			if event.Kind == "run_finished" {
				terminalEvents = append(terminalEvents, event)
			}
		}
		command.Events = terminalEvents
		command.JobSnapshot = ""
		command.ResultReported = true
	}
	if err := store.SaveCommandJournal(journal); err != nil {
		return fmt.Errorf("settle retired command journal: %w", err)
	}
	return store.rotateRetiredEnrollments("", "", retiredEnrollment{
		Endpoint: bootstrap.Endpoint, ServerID: bootstrap.ServerID, Generation: bootstrap.Generation,
		Credential: bootstrap.Credential, ExpiresAt: protocolTimestamp(now.Add(24 * time.Hour)),
	})
}

func (daemon *daemon) reportRetiredJournal(ctx context.Context) error {
	records, err := daemon.store.loadRetiredEnrollments(daemon.bootstrap.CacheKey)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	sort.Slice(records, func(first, second int) bool { return records[first].Generation < records[second].Generation })
	kept := records[:0]
	for _, record := range records {
		expiresAt, _ := time.Parse("2006-01-02T15:04:05.000000Z", record.ExpiresAt)
		if !daemon.now().Before(expiresAt) {
			if err := daemon.purgeRetiredGeneration(record.Generation); err != nil {
				return err
			}
			continue
		}
		complete, reportErr := daemon.flushRetiredGeneration(ctx, record)
		var apiError *APIError
		if errors.As(reportErr, &apiError) && apiError.Code == "enrollment_revoked" {
			if err := daemon.purgeRetiredGeneration(record.Generation); err != nil {
				return err
			}
			continue
		}
		if reportErr != nil {
			if err := daemon.store.saveRetiredEnrollments(daemon.bootstrap.CacheKey, records); err != nil {
				return err
			}
			return reportErr
		}
		if !complete {
			kept = append(kept, record)
		}
	}
	return daemon.store.saveRetiredEnrollments(daemon.bootstrap.CacheKey, kept)
}

func (daemon *daemon) flushRetiredGeneration(ctx context.Context, retired retiredEnrollment) (bool, error) {
	client := NewClient(retired.Endpoint, retired.Credential, daemon.client.Version, daemon.client.HTTP)
	client.Now = daemon.now

	daemon.mu.Lock()
	ids := make([]string, 0)
	for id, command := range daemon.journal.Commands {
		if command.Command.Generation == retired.Generation {
			ids = append(ids, id)
		}
	}
	daemon.mu.Unlock()
	sort.Strings(ids)
	for _, id := range ids {
		if err := daemon.flushRetiredCommand(ctx, client, retired.Generation, id); err != nil {
			var apiError *APIError
			if errors.As(err, &apiError) && apiError.Code == "enrollment_revoked" {
				if purgeErr := daemon.purgeRetiredCommand(id); purgeErr != nil {
					return false, purgeErr
				}
				continue
			}
			return false, err
		}
	}
	return true, nil
}

func (daemon *daemon) flushRetiredCommand(ctx context.Context, client *Client, generation uint64, commandID string) error {
	daemon.mu.Lock()
	command := daemon.journal.Commands[commandID]
	if command == nil || command.Command.Generation != generation {
		daemon.mu.Unlock()
		return nil
	}
	events := append([]AgentEvent(nil), command.Events...)
	runID, logID := command.RunID, command.LogID
	nextChunk, logCompleted := command.NextLogChunk, command.LogCompleted
	logSHA256, logBytes := command.LogSHA256, command.LogBytes
	logTruncated, logDropped := command.LogTruncated, command.LogDroppedBytes
	daemon.mu.Unlock()

	if len(events) > 0 {
		response, err := client.SubmitEvents(ctx, EventRequest{Generation: generation, Events: events})
		if err != nil {
			return err
		}
		for _, result := range response.Results {
			if result.Status == "rejected" {
				return fmt.Errorf("retired event %s was rejected: %s", result.ID, result.Code)
			}
		}
		daemon.mu.Lock()
		if current := daemon.journal.Commands[commandID]; current != nil {
			current.Events = current.Events[len(events):]
			err = daemon.store.SaveCommandJournal(daemon.journal)
		}
		daemon.mu.Unlock()
		if err != nil {
			return err
		}
	}

	if logID != "" && !logCompleted {
		log, err := daemon.store.ReadChecksummedRunLog(logID, logSHA256)
		if err != nil {
			return err
		}
		for offset := nextChunk * maximumLogChunk; offset < len(log); offset += maximumLogChunk {
			end := min(offset+maximumLogChunk, len(log))
			if err := client.UploadLogChunk(ctx, runID, logID, offset/maximumLogChunk, offset, log[offset:end]); err != nil {
				return err
			}
			daemon.mu.Lock()
			if current := daemon.journal.Commands[commandID]; current != nil {
				current.NextLogChunk = offset/maximumLogChunk + 1
				err = daemon.store.SaveCommandJournal(daemon.journal)
			}
			daemon.mu.Unlock()
			if err != nil {
				return err
			}
		}
		completion := LogCompletion{
			Generation: generation, TotalBytes: logBytes, SHA256: logSHA256,
			ChunkCount: (logBytes + maximumLogChunk - 1) / maximumLogChunk,
			Truncated:  logTruncated, DroppedBytes: logDropped,
		}
		if err := client.CompleteLog(ctx, runID, logID, completion); err != nil {
			return err
		}
		daemon.mu.Lock()
		if current := daemon.journal.Commands[commandID]; current != nil {
			current.LogCompleted = true
			err = daemon.store.SaveCommandJournal(daemon.journal)
		}
		daemon.mu.Unlock()
		if err != nil {
			return err
		}
	}

	daemon.mu.Lock()
	current := daemon.journal.Commands[commandID]
	if current != nil && len(current.Events) == 0 && (current.LogID == "" || current.LogCompleted) {
		delete(daemon.journal.Commands, commandID)
		err := daemon.store.SaveCommandJournal(daemon.journal)
		daemon.mu.Unlock()
		if err != nil {
			return err
		}
		if logID != "" {
			return daemon.store.RemoveRunLog(logID)
		}
		return nil
	}
	daemon.mu.Unlock()
	return nil
}

func (daemon *daemon) purgeRetiredGeneration(generation uint64) error {
	daemon.mu.Lock()
	logIDs := make([]string, 0)
	for id, command := range daemon.journal.Commands {
		if command.Command.Generation == generation {
			if command.LogID != "" {
				logIDs = append(logIDs, command.LogID)
			}
			delete(daemon.journal.Commands, id)
		}
	}
	err := daemon.store.SaveCommandJournal(daemon.journal)
	daemon.mu.Unlock()
	if err != nil {
		return err
	}
	for _, logID := range logIDs {
		if err := daemon.store.RemoveRunLog(logID); err != nil {
			return err
		}
	}
	return nil
}

func (daemon *daemon) purgeRetiredCommand(commandID string) error {
	daemon.mu.Lock()
	command := daemon.journal.Commands[commandID]
	logID := ""
	if command != nil {
		logID = command.LogID
		delete(daemon.journal.Commands, commandID)
	}
	err := daemon.store.SaveCommandJournal(daemon.journal)
	daemon.mu.Unlock()
	if err != nil {
		return err
	}
	if logID != "" {
		return daemon.store.RemoveRunLog(logID)
	}
	return nil
}
