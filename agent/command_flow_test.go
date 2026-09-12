package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chieftools/backupchief-agent/restic"
)

type flowExecutor struct {
	mu       sync.Mutex
	requests []restic.Request
	acked    func() bool
	t        *testing.T
}

func (executor *flowExecutor) Run(_ context.Context, request restic.Request) restic.Result {
	if !executor.acked() {
		executor.t.Error("execution started before durable acknowledgement")
	}
	executor.mu.Lock()
	executor.requests = append(executor.requests, request)
	executor.mu.Unlock()
	if request.Operation == "stats" {
		return restic.Result{ExitCode: 0, Outcome: "complete", Output: `{"total_size":4096}`}
	}
	return restic.Result{
		ExitCode: 0,
		Outcome:  "complete",
		Output: fmt.Sprintf(
			`{"message_type":"summary","files_new":1,"dirs_new":1,"total_files_processed":1,"total_bytes_processed":17,"data_added":31,"data_added_packed":23,"snapshot_id":"%s"}`,
			strings.Repeat("d", 64),
		),
	}
}

func TestDaemonCompletesTheDurableCommandReportingFlow(t *testing.T) {
	store := newAgentTestStore(t)
	root := t.TempDir()
	now := time.Now().UTC()
	command := AgentCommand{
		ID:         "01k4p4f7m1r9d3t6v8w2x5y7zb",
		Generation: 1,
		Kind:       "run_backup",
		IssuedAt:   protocolTimestamp(now.Add(-time.Minute)),
		ExpiresAt:  protocolTimestamp(now.Add(10 * time.Minute)),
		Payload: CommandPayload{
			JobID:                  "01k4p4f7m1r9d3t6v8w2x5y7zc",
			RequiredConfigRevision: 2,
		},
	}

	var mu sync.Mutex
	acknowledged := false
	commandDelivered := false
	resultReceived := make(chan CommandResult, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		switch {
		case request.URL.Path == "/agent/v1/config":
			response.WriteHeader(http.StatusNotModified)
		case request.URL.Path == "/agent/v1/heartbeat":
			response.WriteHeader(http.StatusNoContent)
		case request.URL.Path == "/agent/v1/commands":
			mu.Lock()
			deliver := !acknowledged
			commandDelivered = true
			mu.Unlock()
			commands := []AgentCommand{}
			if deliver {
				commands = append(commands, command)
			}
			_ = json.NewEncoder(response).Encode(CommandsResponse{ProtocolRevision: ProtocolRevision, Commands: commands})
		case strings.HasSuffix(request.URL.Path, "/ack"):
			var acknowledgement CommandAcknowledgement
			if err := json.NewDecoder(request.Body).Decode(&acknowledgement); err != nil || !ulidPattern.MatchString(acknowledgement.RunID) {
				t.Errorf("acknowledgement: %+v %v", acknowledgement, err)
			}
			mu.Lock()
			acknowledged = true
			mu.Unlock()
			response.WriteHeader(http.StatusNoContent)
		case request.URL.Path == "/agent/v1/events":
			var events EventRequest
			if err := json.NewDecoder(request.Body).Decode(&events); err != nil {
				t.Errorf("events: %v", err)
			}
			results := make([]EventResult, 0, len(events.Events))
			for _, event := range events.Events {
				results = append(results, EventResult{ID: event.ID, Status: "accepted"})
			}
			_ = json.NewEncoder(response).Encode(EventsResponse{ProtocolRevision: ProtocolRevision, Results: results})
		case strings.Contains(request.URL.Path, "/logs/") && strings.Contains(request.URL.Path, "/chunks/"):
			if request.Header.Get("Content-Type") != "application/octet-stream" {
				t.Errorf("log content type: %s", request.Header.Get("Content-Type"))
			}
			_, _ = io.Copy(io.Discard, request.Body)
			response.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(request.URL.Path, "/complete"):
			response.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(request.URL.Path, "/result"):
			var result CommandResult
			if err := json.NewDecoder(request.Body).Decode(&result); err != nil {
				t.Errorf("result: %v", err)
			}
			response.WriteHeader(http.StatusNoContent)
			select {
			case resultReceived <- result:
			default:
			}
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	bootstrap := testBootstrap()
	bootstrap.Endpoint = server.URL + "/agent/v1"
	if err := store.SaveBootstrap(bootstrap); err != nil {
		t.Fatal(err)
	}
	config := runtimeConfig(root, command.Payload.JobID)
	body, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SaveConfig(bootstrap, body, nil); err != nil {
		t.Fatal(err)
	}
	executor := &flowExecutor{t: t, acked: func() bool {
		mu.Lock()
		defer mu.Unlock()
		return acknowledged
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, RunOptions{
			Store: store, HTTPClient: server.Client(), Version: "1.1.0", Executor: executor,
			HeartbeatEvery: 10 * time.Millisecond, ConfigEvery: 10 * time.Millisecond, CommandEvery: 10 * time.Millisecond,
			ReporterEvery: 10 * time.Millisecond, DispatchEvery: 10 * time.Millisecond,
			Jitter: func(time.Duration) time.Duration { return time.Millisecond },
		})
	}()

	select {
	case result := <-resultReceived:
		if result.Status != "complete" || result.ResultCode != "success" || len(result.SnapshotIDs) != 1 || result.RepositoryBytes == nil || *result.RepositoryBytes != 4096 {
			t.Fatalf("result: %+v", result)
		}
		cancel()
	case <-ctx.Done():
		t.Fatal("command flow timed out")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if !commandDelivered || !acknowledged {
		t.Fatalf("delivery state: delivered=%t acknowledged=%t", commandDelivered, acknowledged)
	}
	mu.Unlock()
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if len(executor.requests) != 2 || executor.requests[0].Root != root || executor.requests[1].Operation != "stats" {
		t.Fatalf("executions: %+v", executor.requests)
	}
}

func runtimeConfig(root, jobID string) Config {
	destinationKey := "storage_01k4p4f7m1r9d3t6v8w2x5y7zb"

	return Config{
		ProtocolRevision: ProtocolRevision,
		Generation:       1,
		Revision:         2,
		SchemaVersion:    ConfigSchemaVersion,
		IssuedAt:         "2026-09-09T08:15:00.000000Z",
		Host:             HostConfig{Name: "synthetic-host", ID: testBootstrap().ServerID},
		Destinations:     map[string]Destination{destinationKey: {Driver: "local", Path: root}},
		Jobs: []Job{{
			ID: jobID, Type: JobTypeFile, Enabled: true,
			Source: JobSource{Root: root, OneFileSystem: true, Excludes: []string{"*.synthetic-cache"}},
			Repository: JobRepository{
				ID: strings.Repeat("e", 64), Destination: destinationKey, Path: "repository",
				ServicePassword: "bcrsvc_SyntheticServicePasswordToken0123456789ABCD0rwHE7",
			},
			Schedule:  JobSchedule{Kind: "preset", Preset: "h24", Expression: "0 1 * * *", Timezone: "UTC"},
			Retention: JobRetention{Last: 12, Daily: 7, Weekly: 4, Monthly: 3, KeepLatestComplete: true, ForgetCron: "17 3 * * *", PruneCron: "47 15 * * 0"},
			Integrity: JobIntegrity{MetadataCron: "0 2 * * 0", DataMode: "auto", DataCron: "0 3 * * 0", DataParts: 4},
		}},
	}
}
