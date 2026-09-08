package agent

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

const criticalRunRecordEstimate = 64 << 10

var fixedUTCParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

func parseFixedUTCSchedule(expression string) (cron.Schedule, error) {
	fields := strings.Fields(expression)
	if len(fields) != 5 || strings.Join(fields, " ") != expression {
		return nil, fmt.Errorf("schedule must use normalized five-field cron")
	}
	for _, field := range fields {
		for _, character := range field {
			if (character < '0' || character > '9') && !strings.ContainsRune("*,-/", character) {
				return nil, fmt.Errorf("schedule fields must be numeric")
			}
		}
	}
	schedule, err := fixedUTCParser.Parse(expression)
	if err != nil {
		return nil, fmt.Errorf("parse schedule: %w", err)
	}
	baseline := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	previous := schedule.Next(baseline)
	for range 512 {
		next := schedule.Next(previous)
		if next.Sub(previous) < 5*time.Minute {
			return nil, fmt.Errorf("schedule must have at least five minutes between occurrences")
		}
		previous = next
	}
	return schedule, nil
}

func scheduleIsDue(expression string, minute time.Time) (bool, error) {
	schedule, err := parseFixedUTCSchedule(expression)
	if err != nil {
		return false, err
	}
	minute = minute.UTC().Truncate(time.Minute)
	return schedule.Next(minute.Add(-time.Minute)).Equal(minute), nil
}

type scheduledOperation struct {
	Job        Job
	RunKind    string
	Expression string
	Priority   int
}

func (daemon *daemon) scheduleBackups(ctx context.Context) error {
	now := daemon.now().UTC()
	minute := now.Truncate(time.Minute)

	daemon.mu.Lock()
	if !minute.After(daemon.lastScheduleMinute) {
		daemon.lastScheduleMinute = minute
		daemon.mu.Unlock()
		return nil
	}
	daemon.lastScheduleMinute = minute
	if daemon.state.AuthenticationPaused {
		daemon.mu.Unlock()
		return nil
	}
	config := daemon.config
	activeBackups, activeMaintenance := daemon.activeCountsLocked()
	busyRepositories := make(map[string]bool, len(daemon.repositories))
	for repositoryID, busy := range daemon.repositories {
		busyRepositories[repositoryID] = busy
	}
	daemon.mu.Unlock()

	operations := make([]scheduledOperation, 0, len(config.Jobs)*5)
	for _, job := range config.Jobs {
		if !job.Enabled {
			continue
		}
		operations = append(operations, scheduledOperation{
			Job:        job,
			RunKind:    "backup",
			Expression: job.Schedule.Expression,
			Priority:   0,
		})
		for _, maintenance := range []scheduledOperation{
			{
				Job:        job,
				RunKind:    "forget",
				Expression: job.Retention.ForgetCron,
				Priority:   1,
			},
			{
				Job:        job,
				RunKind:    "prune",
				Expression: job.Retention.PruneCron,
				Priority:   2,
			},
			{
				Job:        job,
				RunKind:    "check_metadata",
				Expression: job.Integrity.MetadataCron,
				Priority:   3,
			},
			{
				Job:        job,
				RunKind:    "check_data",
				Expression: job.Integrity.DataCron,
				Priority:   4,
			},
		} {
			if maintenance.Expression != "" {
				operations = append(operations, maintenance)
			}
		}
	}
	sort.Slice(operations, func(first, second int) bool {
		if operations[first].Priority != operations[second].Priority {
			return operations[first].Priority < operations[second].Priority
		}
		return operations[first].Job.ID < operations[second].Job.ID
	})

	for _, operation := range operations {
		due, err := scheduleIsDue(operation.Expression, minute)
		if err != nil {
			return err
		}
		if !due {
			continue
		}
		scheduledFor := protocolTimestamp(minute)
		exists, err := daemon.store.occurrenceExists(operation.Job.ID, operation.RunKind, scheduledFor)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		if !daemon.store.canAcceptWork(criticalRunRecordEstimate) {
			if err := daemon.refuseOccurrenceForPressure(operation.Job.ID, operation.RunKind, scheduledFor); err != nil {
				return err
			}
			continue
		}

		runID, err := newULID(now)
		if err != nil {
			return err
		}
		snapshot, err := encodeJobSnapshot(daemon.bootstrap, runID, config.Revision, operation.Job)
		if err != nil {
			return err
		}
		payload := CommandPayload{JobID: operation.Job.ID, RequiredConfigRevision: config.Revision}
		if operation.RunKind != "backup" {
			payload.Maintenance = operation.RunKind
		}
		journaled := &JournalCommand{
			Command: AgentCommand{
				ID: runID, Generation: daemon.bootstrap.Generation, Kind: "scheduled_" + operation.RunKind,
				IssuedAt: scheduledFor, ExpiresAt: protocolTimestamp(minute.Add(24 * time.Hour)), Payload: payload,
			},
			RunID: runID, RunKind: operation.RunKind, ConfigRevision: config.Revision,
			Trigger: "scheduled", ScheduledFor: scheduledFor, JobSnapshot: snapshot,
			ReceivedAt: protocolTimestamp(now), Acknowledged: true, State: "received", Events: []AgentEvent{},
		}

		overlap := busyRepositories[operation.Job.Repository.ID]
		if operation.RunKind == "backup" {
			overlap = overlap || activeBackups >= 2
		} else {
			overlap = overlap || activeMaintenance >= 1
		}
		if overlap {
			daemon.finishScheduledOverlap(journaled, now)
		}
		daemon.mu.Lock()
		daemon.journal.Commands[runID] = journaled
		err = daemon.store.SaveCommandJournal(daemon.journal)
		if err != nil {
			delete(daemon.journal.Commands, runID)
		}
		daemon.mu.Unlock()
		if err != nil {
			if errors.Is(err, ErrSpoolCapacity) {
				if gapErr := daemon.refuseOccurrenceForPressure(operation.Job.ID, operation.RunKind, scheduledFor); gapErr != nil {
					return gapErr
				}
				continue
			}
			return err
		}
		if overlap {
			continue
		}
		busyRepositories[operation.Job.Repository.ID] = true
		if operation.RunKind == "backup" {
			activeBackups++
		} else {
			activeMaintenance++
		}
		if err := daemon.startOperation(ctx, runID, operation.Job); err != nil {
			return err
		}
	}
	return nil
}

func (daemon *daemon) finishScheduledOverlap(command *JournalCommand, now time.Time) {
	timestamp := protocolTimestamp(now)
	command.State = "finished"
	command.ResultReported = true
	command.Result = &CommandResult{
		Generation: daemon.bootstrap.Generation, RunID: command.RunID, JobID: command.Command.Payload.JobID,
		RunKind: command.RunKind, Status: "skipped", ResultCode: "schedule_overlap",
		StartedAt: timestamp, FinishedAt: timestamp, SnapshotIDs: []string{},
		Summary: "The scheduled operation was skipped because execution capacity was unavailable.",
	}
	command.Sequence = 1
	if eventID, err := newULID(now); err == nil {
		command.Events = append(command.Events, daemon.eventEnvelope(
			command, eventID, command.Sequence, timestamp, "run_finished", terminalEventPayload(*command.Result),
		))
	}
}

func (daemon *daemon) refuseOccurrenceForPressure(jobID, runKind, scheduledFor string) error {
	err := daemon.store.recordSkippedOccurrence(jobID, runKind, scheduledFor)
	if errors.Is(err, ErrSpoolCapacity) {
		err = daemon.store.recordSkippedOccurrence(jobID, runKind, scheduledFor)
	}
	if err != nil {
		return err
	}
	daemon.mu.Lock()
	daemon.state.SpoolGapDetected = true
	daemon.mu.Unlock()
	return nil
}

func (daemon *daemon) eventEnvelope(
	command *JournalCommand,
	eventID string,
	sequence uint64,
	occurredAt string,
	kind string,
	payload map[string]any,
) AgentEvent {
	return AgentEvent{
		ID: eventID, RunID: command.RunID, JobID: command.Command.Payload.JobID, RunKind: command.RunKind,
		ConfigRevision: command.ConfigRevision, Trigger: command.Trigger, ScheduledFor: command.ScheduledFor,
		Sequence: sequence, OccurredAt: occurredAt, Kind: kind, Payload: payload,
	}
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
