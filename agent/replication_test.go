package agent

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/chieftools/backupchief-agent/restic"
)

func TestReplicationCoalescesPendingRunsAndReportsTheirSnapshotMappings(t *testing.T) {
	store := newAgentTestStore(t)
	firstSnapshot := strings.Repeat("a", 64)
	secondSnapshot := strings.Repeat("b", 64)
	replicaSnapshot := strings.Repeat("e", 64)
	commandIDs := []string{"01k4p4f7m1r9d3t6v8w2x5y7zc", "01k4p4f7m1r9d3t6v8w2x5y7zd"}
	replicaKey := "repository_01k4p4f7m1r9d3t6v8w2x5y7zf"
	journal := newCommandJournal()
	for index, commandID := range commandIDs {
		snapshotID := []string{firstSnapshot, secondSnapshot}[index]
		journal.Commands[commandID] = &JournalCommand{
			Command: AgentCommand{ID: commandID, Payload: CommandPayload{JobID: "01k4p4f7m1r9d3t6v8w2x5y7za"}},
			RunID:   []string{"01k4p4f7m1r9d3t6v8w2x5y7ze", "01k4p4f7m1r9d3t6v8w2x5y7zg"}[index],
			RunKind: "backup", State: "finished", Events: []AgentEvent{},
			Result:             &CommandResult{JobID: "01k4p4f7m1r9d3t6v8w2x5y7za", SnapshotIDs: []string{snapshotID}},
			ReplicationPending: []string{replicaKey},
		}
	}
	executor := &sequentialExecutor{results: []restic.Result{
		{ExitCode: 0, Outcome: "complete"},
		{ExitCode: 0, Outcome: "complete", Output: `[{"id":"` + replicaSnapshot + `","original":"` + firstSnapshot + `"},{"id":"` + secondSnapshot + `"}]`},
		{ExitCode: 0, Outcome: "complete", Output: `{"total_size":8192}`},
	}}
	job := executionJob(t.TempDir())
	job.ID = "01k4p4f7m1r9d3t6v8w2x5y7za"
	job.Repository.Key = "repository_01k4p4f7m1r9d3t6v8w2x5y7ze"
	job.Replicas = []JobRepository{{
		Key: replicaKey, ID: strings.Repeat("d", 64), ServicePassword: "synthetic-service-password", Source: job.Repository.Key, Status: "active",
		Connection: testLocalRepository(t.TempDir()),
	}}
	runtime := &daemon{
		store: store, now: func() time.Time { return time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC) },
		journal: journal, executor: executor, replicationActive: map[string]bool{}, reportWake: make(chan struct{}, 1),
	}

	runtime.startReplication(context.Background(), job)
	runtime.activeWG.Wait()

	if len(executor.requests) != 3 || executor.requests[0].Operation != "copy" {
		t.Fatalf("requests: %+v", executor.requests)
	}
	for _, commandID := range commandIDs {
		command := runtime.journal.Commands[commandID]
		if len(command.ReplicationPending) != 0 || len(command.Events) != 1 || command.Events[0].Kind != "replication_finished" || command.Events[0].Payload["status"] != "complete" {
			t.Fatalf("replication result for %s: %+v", commandID, command)
		}
	}
}

func TestReplicationReportsABoundedFailureDiagnostic(t *testing.T) {
	store := newAgentTestStore(t)
	commandID := "01k4p4f7m1r9d3t6v8w2x5y7zh"
	replicaKey := "repository_01k4p4f7m1r9d3t6v8w2x5y7zj"
	journal := newCommandJournal()
	journal.Commands[commandID] = &JournalCommand{
		Command: AgentCommand{ID: commandID, Payload: CommandPayload{JobID: "01k4p4f7m1r9d3t6v8w2x5y7zk"}},
		RunID:   "01k4p4f7m1r9d3t6v8w2x5y7zm", RunKind: "backup", State: "finished", Events: []AgentEvent{},
		Result:             &CommandResult{JobID: "01k4p4f7m1r9d3t6v8w2x5y7zk", SnapshotIDs: []string{strings.Repeat("c", 64)}},
		ReplicationPending: []string{replicaKey},
	}
	diagnostic := "synthetic copy failure " + strings.Repeat("x", 5000)
	executor := &sequentialExecutor{results: []restic.Result{{ExitCode: 1, Outcome: "failed", Diagnostic: diagnostic}}}
	job := executionJob(t.TempDir())
	job.ID = "01k4p4f7m1r9d3t6v8w2x5y7zk"
	job.Replicas = []JobRepository{{
		Key: replicaKey, ID: strings.Repeat("d", 64), ServicePassword: "synthetic-service-password", Source: job.Repository.Key, Status: "active",
		Connection: testLocalRepository(t.TempDir()),
	}}
	runtime := &daemon{
		store: store, now: func() time.Time { return time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC) },
		journal: journal, executor: executor, replicationActive: map[string]bool{}, reportWake: make(chan struct{}, 1),
	}

	runtime.startReplication(context.Background(), job)
	runtime.activeWG.Wait()

	command := runtime.journal.Commands[commandID]
	reported, ok := command.Events[0].Payload["diagnostic"].(string)
	if !ok || !strings.HasPrefix(reported, "synthetic copy failure") || len([]rune(reported)) != 4096 {
		t.Fatalf("diagnostic: %q", reported)
	}
	if command.Events[0].Payload["status"] != "failed" || len(command.ReplicationPending) != 1 {
		t.Fatalf("replication result: %+v", command)
	}
}

func TestPendingReplicationBlocksDeferredMaintenance(t *testing.T) {
	store := newAgentTestStore(t)
	job := maintenanceExecutionJob(t)
	maintenanceID := "01k4p4f7m1r9d3t6v8w2x5y7zj"
	backupID := "01k4p4f7m1r9d3t6v8w2x5y7zk"
	maintenance := maintenanceJournalCommand("check_metadata", job.ID)
	maintenance.Command.ID = maintenanceID
	maintenance.State = "received"
	journal := newCommandJournal()
	journal.Commands[maintenanceID] = maintenance
	journal.Commands[backupID] = &JournalCommand{
		Command: AgentCommand{ID: backupID, Payload: CommandPayload{JobID: job.ID}},
		RunID:   "01k4p4f7m1r9d3t6v8w2x5y7zm", RunKind: "backup", State: "finished",
		Result: &CommandResult{JobID: job.ID}, ReplicationPending: []string{"repository_01k4p4f7m1r9d3t6v8w2x5y7zn"},
	}
	executor := &schedulerExecutor{}
	runtime := &daemon{
		store: store, bootstrap: testBootstrap(), now: time.Now, journal: journal, executor: executor,
		active: map[string]context.CancelFunc{}, activeRunKinds: map[string]string{}, repositories: map[string]bool{},
		replicationActive: map[string]bool{}, state: RuntimeState{Maintenance: map[string]MaintenanceRuntime{}},
	}

	if err := runtime.startOperation(context.Background(), maintenanceID, job); err != nil {
		t.Fatal(err)
	}
	if executor.count() != 0 || runtime.journal.Commands[maintenanceID].State != "received" {
		t.Fatalf("maintenance started with replication pending: %+v", runtime.journal.Commands[maintenanceID])
	}

	runtime.mu.Lock()
	runtime.journal.Commands[backupID].ReplicationPending = nil
	runtime.mu.Unlock()
	if err := runtime.startOperation(context.Background(), maintenanceID, job); err != nil {
		t.Fatal(err)
	}
	runtime.activeWG.Wait()
	if executor.count() != 1 || runtime.journal.Commands[maintenanceID].Result == nil || runtime.journal.Commands[maintenanceID].Result.Status != "complete" {
		t.Fatalf("maintenance did not start after replication completed: %+v", runtime.journal.Commands[maintenanceID])
	}
}

func TestInitialReplicaSyncUsesProvisioningConfigurationAndReportsItsTarget(t *testing.T) {
	store := newAgentTestStore(t)
	commandID := "01k4p4f7m1r9d3t6v8w2x5y7zn"
	replicaKey := "repository_01k4p4f7m1r9d3t6v8w2x5y7zp"
	journal := newCommandJournal()
	journal.Commands[commandID] = &JournalCommand{
		Command: AgentCommand{ID: commandID, Kind: "sync_replica", Payload: CommandPayload{
			JobID: "01k4p4f7m1r9d3t6v8w2x5y7zq", RepositoryKey: replicaKey,
		}},
		RunID: "01k4p4f7m1r9d3t6v8w2x5y7zr", RunKind: "replica_sync", State: "received", Events: []AgentEvent{},
	}
	executor := &sequentialExecutor{results: []restic.Result{
		{ExitCode: 0, Outcome: "complete"},
		{ExitCode: 0, Outcome: "complete", Output: `[]`},
		{ExitCode: 0, Outcome: "complete", Output: `{"total_size":4096}`},
	}}
	job := executionJob(t.TempDir())
	job.ID = journal.Commands[commandID].Command.Payload.JobID
	job.Repository.Key = "repository_01k4p4f7m1r9d3t6v8w2x5y7zs"
	job.ReplicaSetups = []JobRepository{{
		Key: replicaKey, ID: strings.Repeat("e", 64), ServicePassword: "synthetic-service-password",
		Source: job.Repository.Key, Status: "provisioning", Connection: testLocalRepository(t.TempDir()),
	}}
	runtime := &daemon{
		store: store, bootstrap: Bootstrap{Generation: 1}, now: time.Now, journal: journal, executor: executor,
		replicationActive: map[string]bool{}, repositories: map[string]bool{}, reportWake: make(chan struct{}, 1),
	}

	if err := runtime.startReplicaSync(context.Background(), commandID, job); err != nil {
		t.Fatal(err)
	}
	runtime.activeWG.Wait()

	result := runtime.journal.Commands[commandID].Result
	if result == nil || result.Status != "complete" || result.RepositoryKey != replicaKey || result.RepositoryBytes == nil || *result.RepositoryBytes != 4096 {
		t.Fatalf("replica sync result: %+v", result)
	}
	if len(executor.requests) != 3 || executor.requests[0].Operation != "copy" || executor.requests[1].Operation != "snapshots" || executor.requests[2].Operation != "stats" {
		t.Fatalf("replica sync requests: %+v", executor.requests)
	}
}
