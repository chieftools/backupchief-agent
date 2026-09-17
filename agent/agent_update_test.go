package agent

import (
	"context"
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
	if runtime.journal.Commands[command.ID].State != "running" {
		t.Fatal("update was not handed to the privileged helper")
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
