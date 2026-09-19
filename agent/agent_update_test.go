package agent

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/chieftools/backupchief-agent/updater"
)

func TestAgentUpdateDrainsBeforeWritingThePrivilegedRequest(t *testing.T) {
	store := newAgentTestStore(t)
	now := time.Date(2026, 9, 17, 11, 0, 0, 0, time.UTC)
	runtime := &daemon{
		store: store, bootstrap: Bootstrap{Generation: 2}, version: "5.1.0", now: func() time.Time { return now },
		journal: newCommandJournal(), active: map[string]context.CancelFunc{"synthetic-run": func() {}},
		repositories: map[string]bool{}, replicationActive: map[string]bool{}, dispatchWake: make(chan struct{}, 1),
	}
	command := AgentCommand{
		ID: "01k4p4f7m1r9d3t6v8w2x5y7za", Generation: 2, Kind: "update_agent",
		IssuedAt: protocolTimestamp(now), ExpiresAt: protocolTimestamp(now.Add(time.Hour)),
		Payload: CommandPayload{TargetVersion: "5.2.0"},
	}

	if err := runtime.receiveCommand(command); err != nil {
		t.Fatal(err)
	}
	if runtime.state.AgentUpdate == nil {
		t.Fatal("update drain was not persisted")
	}
	journaledRunID := runtime.journal.Commands[command.ID].RunID
	if runtime.state.AgentUpdate.RunID != journaledRunID || journaledRunID == command.ID {
		t.Fatal("update drain and command journal do not share the generated run identity")
	}
	if err := runtime.startAgentUpdate(command.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(updater.RequestPath(store.stateDirectory())); !os.IsNotExist(err) {
		t.Fatal("updater request was written while work was active")
	}

	delete(runtime.active, "synthetic-run")
	if err := runtime.startAgentUpdate(command.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(updater.RequestPath(store.stateDirectory())); err != nil {
		t.Fatal(err)
	}
	requestData, err := os.ReadFile(updater.RequestPath(store.stateDirectory()))
	if err != nil {
		t.Fatal(err)
	}
	var request updater.Request
	if err := json.Unmarshal(requestData, &request); err != nil {
		t.Fatal(err)
	}
	if request.RunID != journaledRunID {
		t.Fatal("updater request does not use the journal run identity")
	}
	if runtime.journal.Commands[command.ID].State != "running" {
		t.Fatal("update was not handed to the privileged helper")
	}
}

func TestAgentUpdateRestartRepairsLegacyRunIdentityAndConsumesResult(t *testing.T) {
	store := newAgentTestStore(t)
	now := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)
	commandID := "01k4p4f7m1r9d3t6v8w2x5y7za"
	runID := "01k4p4f7m1r9d3t6v8w2x5y7zb"
	command := AgentCommand{
		ID: commandID, Generation: 3, Kind: "update_agent",
		IssuedAt: protocolTimestamp(now.Add(-time.Minute)), ExpiresAt: protocolTimestamp(now.Add(time.Hour)),
		Payload: CommandPayload{TargetVersion: "5.2.1"},
	}
	journal := newCommandJournal()
	journal.Commands[commandID] = &JournalCommand{
		Command: command, RunID: runID, RunKind: "agent_update", Trigger: "manual",
		ReceivedAt: protocolTimestamp(now.Add(-30 * time.Second)), Acknowledged: true, State: "running", Events: []AgentEvent{},
	}
	runtime := &daemon{
		store: store, bootstrap: Bootstrap{Generation: 3}, version: "5.2.1", now: func() time.Time { return now }, journal: journal,
		state: RuntimeState{AgentUpdate: &AgentUpdateRuntime{
			CommandID: commandID, RunID: commandID, TargetVersion: "5.2.1",
			StartedAt: protocolTimestamp(now.Add(-30 * time.Second)), LastScheduleMinute: protocolTimestamp(now.Add(-time.Minute)),
		}},
	}
	if err := store.SaveCommandJournal(runtime.journal); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveRuntimeState(runtime.state); err != nil {
		t.Fatal(err)
	}

	if err := runtime.restoreAgentUpdateState(); err != nil {
		t.Fatal(err)
	}
	if runtime.state.AgentUpdate == nil || runtime.state.AgentUpdate.RunID != runID {
		t.Fatal("legacy update identity was not repaired from the journal")
	}
	persisted, err := store.LoadRuntimeState()
	if err != nil {
		t.Fatal(err)
	}
	if persisted.AgentUpdate == nil || persisted.AgentUpdate.RunID != runID {
		t.Fatal("repaired update identity was not persisted")
	}

	result := updater.Result{
		Generation: 3, RunID: runID, Status: "failed", ResultCode: "readiness_failed_rolled_back",
		PreviousVersion: "5.1.0", TargetVersion: "5.2.1", InstalledVersion: "5.1.0",
		StartedAt: protocolTimestamp(now.Add(-30 * time.Second)), FinishedAt: protocolTimestamp(now),
	}
	resultData, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(updater.ResultPath(store.stateDirectory()), append(resultData, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runtime.consumeAgentUpdateResult(); err != nil {
		t.Fatal(err)
	}
	if runtime.state.AgentUpdate != nil {
		t.Fatal("agent update state was not cleared")
	}
	journaled := runtime.journal.Commands[commandID]
	if journaled.State != "finished" || journaled.UpdateResult == nil || journaled.UpdateResult.RunID != runID {
		t.Fatal("updater result was not recorded against the repaired command")
	}
	if _, err := os.Stat(updater.ResultPath(store.stateDirectory())); !os.IsNotExist(err) {
		t.Fatal("consumed updater result was not removed")
	}
}

func TestAgentUpdateRestartClearsOrphanedState(t *testing.T) {
	store := newAgentTestStore(t)
	commandID := "01k4p4f7m1r9d3t6v8w2x5y7za"
	runtime := &daemon{
		store: store, journal: newCommandJournal(),
		state: RuntimeState{AgentUpdate: &AgentUpdateRuntime{
			CommandID: commandID, RunID: "01k4p4f7m1r9d3t6v8w2x5y7zb", TargetVersion: "5.2.1",
			StartedAt: "2026-09-19T10:00:00.000000Z",
		}},
	}
	if err := store.SaveRuntimeState(runtime.state); err != nil {
		t.Fatal(err)
	}

	if err := runtime.restoreAgentUpdateState(); err != nil {
		t.Fatal(err)
	}
	if runtime.state.AgentUpdate != nil {
		t.Fatal("orphaned update state was not cleared")
	}
	persisted, err := store.LoadRuntimeState()
	if err != nil {
		t.Fatal(err)
	}
	if persisted.AgentUpdate != nil {
		t.Fatal("orphaned update state cleanup was not persisted")
	}
}

func TestExpiredUnacknowledgedManualCommandIsDiscarded(t *testing.T) {
	store := newAgentTestStore(t)
	now := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)
	expiredID := "01k4p4f7m1r9d3t6v8w2x5y7za"
	futureID := "01k4p4f7m1r9d3t6v8w2x5y7zb"
	journal := newCommandJournal()
	for id, expiresAt := range map[string]time.Time{expiredID: now.Add(-time.Minute), futureID: now.Add(time.Hour)} {
		journal.Commands[id] = &JournalCommand{
			Command: AgentCommand{ID: id, Generation: 4, Kind: "run_maintenance", ExpiresAt: protocolTimestamp(expiresAt)},
			RunID:   id, RunKind: "check", Trigger: "manual", ReceivedAt: protocolTimestamp(now.Add(-time.Hour)), State: "received", Events: []AgentEvent{},
		}
	}
	runtime := &daemon{store: store, now: func() time.Time { return now }, journal: journal}
	if err := store.SaveCommandJournal(runtime.journal); err != nil {
		t.Fatal(err)
	}

	discarded, err := runtime.discardExpiredUnacknowledgedCommand(expiredID)
	if err != nil {
		t.Fatal(err)
	}
	if !discarded || runtime.journal.Commands[expiredID] != nil {
		t.Fatal("expired unacknowledged command was not discarded")
	}
	discarded, err = runtime.discardExpiredUnacknowledgedCommand(futureID)
	if err != nil {
		t.Fatal(err)
	}
	if discarded || runtime.journal.Commands[futureID] == nil {
		t.Fatal("unexpired command was discarded")
	}
}

func TestSchedulesRemainDurableWhileAnAgentUpdateIsDraining(t *testing.T) {
	store := newAgentTestStore(t)
	job := executionJob(t.TempDir())
	job.Schedule = JobSchedule{Kind: "cron", Expression: "*/5 * * * *", Timezone: "UTC"}
	now := time.Date(2026, 9, 18, 0, 59, 0, 0, time.UTC)
	executor := &schedulerExecutor{}
	runtime := newSchedulerDaemon(t, store, schedulerConfig(job), &now, executor)
	runtime.state.AgentUpdate = &AgentUpdateRuntime{
		CommandID: "01k4p4f7m1r9d3t6v8w2x5y7za", RunID: "01k4p4f7m1r9d3t6v8w2x5y7zb",
		TargetVersion: "5.2.0", StartedAt: protocolTimestamp(now),
	}

	now = time.Date(2026, 9, 18, 1, 0, 0, 0, time.UTC)
	if err := runtime.scheduleBackups(context.Background()); err != nil {
		t.Fatal(err)
	}
	if executor.count() != 0 {
		t.Fatal("scheduled work started during update drain")
	}
	now = time.Date(2026, 9, 18, 1, 5, 0, 0, time.UTC)
	if err := runtime.scheduleBackups(context.Background()); err != nil {
		t.Fatal(err)
	}
	queued := 0
	for _, command := range runtime.journal.Commands {
		if command.Command.Kind == "scheduled_backup" && command.State == "received" {
			queued++
		}
	}
	if queued != 1 {
		t.Fatalf("queued scheduled backups: %d", queued)
	}
}
