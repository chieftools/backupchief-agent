package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/chieftools/backupchief-agent/restic"
)

func activityTestDaemon(t *testing.T, handler http.HandlerFunc) (*daemon, *JournalCommand, Job) {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	daemon := &daemon{
		now:       time.Now,
		bootstrap: Bootstrap{Generation: 1},
		client:    NewClient(server.URL, "synthetic-credential", "9.0.0", server.Client()),
	}
	command := &JournalCommand{
		RunID:   "01k4p4f7m1r9d3t6v8w2x5y7za",
		Command: AgentCommand{ID: "01k4p4f7m1r9d3t6v8w2x5y7zb"},
	}
	job := Job{ID: "01k4p4f7m1r9d3t6v8w2x5y7zc"}

	return daemon, command, job
}

func TestActivityCoalescesReportsAndStopsWhenClosed(t *testing.T) {
	var batches [][]ActivitySnapshot
	daemon, command, job := activityTestDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Generation uint64             `json:"generation"`
			Activities []ActivitySnapshot `json:"activities"`
		}

		if r.URL.Path != "/activity" || json.NewDecoder(r.Body).Decode(&payload) != nil || payload.Generation != 1 {
			t.Error("invalid activity request")
		}

		batches = append(batches, payload.Activities)
		w.Header().Set(ProtocolHeader, ProtocolRevision)
		w.WriteHeader(http.StatusNoContent)
	})

	activity := daemon.beginActivity("run", []*JournalCommand{command}, job, "")
	for i := range 1000 {
		activity.update("backing_up", map[string]uint64{"bytes_processed": uint64(i)})
	}

	if err := daemon.reportActivity(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err := daemon.reportActivity(context.Background()); err != nil {
		t.Fatal(err)
	}

	activity.close()
	if err := daemon.reportActivity(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(batches) != 2 || len(batches[0]) != 1 {
		t.Fatalf("invalid batches: %+v", batches)
	}

	if batches[0][0].Counters["bytes_processed"] != 999 ||
		batches[1][0].Sequence != 2 ||
		batches[0][0].RunID != "run_"+command.RunID {
		t.Fatalf("invalid samples: %+v", batches)
	}

	if len(command.Events) != 0 {
		t.Fatal("activity entered durable journal")
	}
}

func TestActivitySkipsOlderPanelsAndContinuesAfterReportingFailure(t *testing.T) {
	requests := 0
	daemon, command, job := activityTestDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set(ProtocolHeader, ProtocolRevision)
		w.WriteHeader(http.StatusServiceUnavailable)
	})

	activity := daemon.beginActivity("run", []*JournalCommand{command}, job, "")
	defer activity.close()

	daemon.client.selectProtocolRevision("1.15.0")
	if err := daemon.reportActivity(context.Background()); err != nil || requests != 0 {
		t.Fatal("contacted incompatible panel")
	}
	daemon.client.selectProtocolRevision(ProtocolRevision)
	if err := daemon.reportActivity(context.Background()); err == nil {
		t.Fatal("expected temporary reporting error")
	}

	activity.update("measuring_repository", map[string]uint64{"bytes_processed": 1024})
	if len(daemon.activities) != 1 || len(command.Events) != 0 {
		t.Fatal("report failure changed execution state")
	}
}

func TestActivityExecutorAggregatesDatabaseBytes(t *testing.T) {
	daemon, command, job := activityTestDaemon(t, func(w http.ResponseWriter, r *http.Request) {})
	activity := daemon.beginActivity("run", []*JournalCommand{command}, job, "")
	defer activity.close()

	activity.update("backing_up", map[string]uint64{"databases_completed": 0, "databases_total": 2})
	executor := &activityExecutor{
		activity: activity,
		executor: &sequentialExecutor{results: []restic.Result{
			{
				ExitCode: 0,
				Outcome:  "complete",
				Output:   `{"message_type":"summary","total_bytes_processed":4096,"total_files_processed":1}`,
			},
			{
				ExitCode: 0,
				Outcome:  "complete",
				Output:   `{"message_type":"summary","total_bytes_processed":8192,"total_files_processed":1}`,
			},
		}},
	}

	executor.Run(context.Background(), restic.Request{Operation: "backup_stdin"})
	executor.Run(context.Background(), restic.Request{Operation: "backup_stdin"})

	snapshot := daemon.activities[activity.keys[0]]
	if snapshot.Counters["bytes_processed"] != 12288 ||
		snapshot.Counters["files_processed"] != 2 ||
		snapshot.Counters["databases_total"] != 2 {
		t.Fatalf("wrong cumulative counters: %+v", snapshot)
	}
}

func TestReplicationActivityKeepsRecoveryPointsSeparate(t *testing.T) {
	daemon, command, job := activityTestDaemon(t, func(w http.ResponseWriter, r *http.Request) {})
	second := *command
	second.RunID = "01k4p4f7m1r9d3t6v8w2x5y7zd"

	activity := daemon.beginActivity(
		"replication",
		[]*JournalCommand{command, &second},
		job,
		"repository_01k4p4f7m1r9d3t6v8w2x5y7ze",
	)
	defer activity.close()

	activity.update("verifying_snapshots", nil)
	if len(daemon.activities) != 2 {
		t.Fatal("replication batch lost recovery points")
	}

	for _, snapshot := range daemon.activities {
		if snapshot.Kind != "replication" || snapshot.RepositoryKey == "" || snapshot.Stage != "verifying_snapshots" {
			t.Fatalf("invalid replication identity: %+v", snapshot)
		}
	}
}
