package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestClientNegotiatesDownAfterRollbackAndBackUpAfterUpgrade(t *testing.T) {
	var requests atomic.Int32
	var upgraded atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		attempt := requests.Add(1)
		revision := request.Header.Get(ProtocolHeader)
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}

		if !upgraded.Load() && attempt == 1 {
			if revision != ProtocolRevision {
				t.Fatalf("initial protocol revision: %q", revision)
			}
			response.Header().Set(ProtocolHeader, "1.0.0")
			response.Header().Set("Content-Type", "application/problem+json")
			response.WriteHeader(http.StatusUpgradeRequired)
			_, _ = response.Write([]byte(`{"type":"https://backup.example.test/problems/unsupported_protocol","title":"Unsupported protocol revision","status":426,"code":"unsupported_protocol"}`))
			return
		}

		if !upgraded.Load() && revision != "1.0.0" {
			t.Fatalf("fallback protocol revision: %q", revision)
		}
		if revision == "1.0.0" {
			if _, exists := payload["capabilities"]; exists {
				t.Fatalf("1.0.0 heartbeat contains capabilities: %#v", payload)
			}
			config, _ := payload["config"].(map[string]any)
			if _, exists := config["protocol_revision"]; exists {
				t.Fatalf("1.0.0 heartbeat contains config protocol revision: %#v", payload)
			}
		} else if _, exists := payload["capabilities"]; !exists {
			t.Fatalf("1.1.0 heartbeat omitted capabilities: %#v", payload)
		} else if config, _ := payload["config"].(map[string]any); config["protocol_revision"] != ProtocolRevision {
			t.Fatalf("1.1.0 heartbeat config protocol revision: %#v", payload)
		}
		if upgraded.Load() {
			if attempt == 3 && revision != "1.0.0" {
				t.Fatalf("upgrade discovery revision: %q", revision)
			}
			if attempt == 4 && revision != ProtocolRevision {
				t.Fatalf("upgraded protocol revision: %q", revision)
			}
			response.Header().Set(LatestProtocolHeader, ProtocolRevision)
		}
		response.Header().Set(ProtocolHeader, revision)
		response.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client := NewClient(server.URL, testBootstrap().Credential, "1.2.3-test", server.Client())
	heartbeat := HeartbeatRequest{
		Config:       HeartbeatConfig{ProtocolRevision: ProtocolRevision},
		Capabilities: map[string]any{"backup_types": map[string]any{"file": map[string]any{"available": true}}},
	}

	if err := client.Heartbeat(context.Background(), heartbeat); err != nil {
		t.Fatalf("heartbeat after rollback negotiation: %v", err)
	}
	upgraded.Store(true)
	if err := client.Heartbeat(context.Background(), heartbeat); err != nil {
		t.Fatalf("heartbeat during upgrade discovery: %v", err)
	}
	if err := client.Heartbeat(context.Background(), heartbeat); err != nil {
		t.Fatalf("heartbeat after upgrade negotiation: %v", err)
	}
	if requests.Load() != 4 {
		t.Fatalf("requests: %d, want 4", requests.Load())
	}
}

func TestEnrollmentFallbackUsesTheServerRevisionInItsHeaderAndBody(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		attempt := requests.Add(1)
		var payload EnrollmentRequest
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if attempt == 1 {
			response.Header().Set(ProtocolHeader, "1.0.0")
			response.WriteHeader(http.StatusUpgradeRequired)
			return
		}
		if request.Header.Get(ProtocolHeader) != "1.0.0" || payload.ProtocolRevision != "1.0.0" {
			t.Fatalf("fallback request used header %q and body %q", request.Header.Get(ProtocolHeader), payload.ProtocolRevision)
		}
		response.Header().Set(ProtocolHeader, "1.0.0")
		response.WriteHeader(http.StatusCreated)
		_, _ = response.Write([]byte(`{"protocol_revision":"1.0.0","server_id":"01k4p4f7m1r9d3t6v8w2x5y7za","generation":1,"enrolled_at":"2026-09-11T10:30:00.000000Z","config_revision":1}`))
	}))
	defer server.Close()
	client := NewClient(server.URL, "", "1.2.3-test", server.Client())

	result, err := client.Enroll(context.Background(), "bcenr_syntheticEnrollmentToken1234567890", EnrollmentRequest{
		ProtocolRevision: ProtocolRevision,
		AttemptID:        "01k4p4g7m1r9d3t6v8w2x5y7zb",
		AgentVersion:     "1.2.3-test",
		Hostname:         "agent.example.test",
		Platform:         "linux",
		Architecture:     "arm64",
		Credential:       "synthetic-credential-value",
	})

	if err != nil || result.ProtocolRevision != "1.0.0" {
		t.Fatalf("enroll after fallback: %+v %v", result, err)
	}
}

func TestInspectionResultUsesItsDedicatedWireShape(t *testing.T) {
	commandID := "01k4p4f7m1r9d3t6v8w2x5y7za"
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload["status"] != "complete" || payload["summary"] != "Synthetic source verified." {
			t.Fatalf("inspection payload: %#v", payload)
		}
		for _, unexpected := range []string{"job_id", "run_kind", "started_at", "finished_at", "snapshot_ids", "statistics", "artifacts", "failure"} {
			if _, exists := payload[unexpected]; exists {
				t.Fatalf("inspection payload contains %s: %#v", unexpected, payload)
			}
		}
		response.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client := NewClient(server.URL, testBootstrap().Credential, "1.2.3-test", server.Client())

	err := client.SubmitInspectionResult(context.Background(), commandID, CommandResult{
		Generation: 1,
		RunID:      "01k4p4k2n8d3r6t9v1w5x7yabc",
		Status:     "complete",
		ResultCode: "success",
		Summary:    "Synthetic source verified.",
		Databases:  []string{"synthetic_app"},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestInspectionFailureUsesTheNegotiatedWireShape(t *testing.T) {
	tests := []struct {
		name          string
		revision      string
		failure       *SourceInspectionFailure
		wantStage     string
		wantNoFailure bool
	}{
		{name: "protocol 1.2", revision: "1.2.0", failure: &SourceInspectionFailure{Stage: "dump_test", Detail: "Synthetic dump failure."}, wantStage: "dump_test"},
		{name: "protocol 1.1", revision: "1.1.0", failure: &SourceInspectionFailure{Stage: "dump_test", Detail: "Synthetic dump failure."}, wantNoFailure: true},
		{name: "upgraded journal", revision: "1.2.0", wantStage: "unknown"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			commandID := "01k4p4f7m1r9d3t6v8w2x5y7za"
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				response.Header().Set(ProtocolHeader, test.revision)
				var payload map[string]any
				if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
					t.Fatal(err)
				}
				failure, exists := payload["failure"].(map[string]any)
				if test.wantNoFailure {
					if exists {
						t.Fatalf("legacy payload contains failure: %#v", payload)
					}
				} else if !exists || failure["stage"] != test.wantStage || failure["detail"] == "" {
					t.Fatalf("failure payload: %#v", payload)
				}
				response.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			client := NewClient(server.URL, testBootstrap().Credential, "1.2.3-test", server.Client())
			client.selectProtocolRevision(test.revision)

			err := client.SubmitInspectionResult(context.Background(), commandID, CommandResult{
				Generation: 1,
				RunID:      "01k4p4k2n8d3r6t9v1w5x7yabc",
				Status:     "failed",
				ResultCode: "source_authentication_failed",
				Summary:    "Synthetic source check failed.",
				Failure:    test.failure,
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestManagedBackupClientQueuesAndWaitsForCompletion(t *testing.T) {
	commandID := "01k4p4f7m1r9d3t6v8w2x5y7za"
	jobID := "01k4p4g2m7d9r3t6v8w1x5y2zb"
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		switch request.Method + " " + request.URL.Path {
		case "POST /agent/v1/backups":
			var payload map[string]string
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil || payload["job_id"] != jobID {
				t.Fatalf("unexpected backup request: %v %+v", err, payload)
			}
			response.WriteHeader(http.StatusAccepted)
			_, _ = response.Write([]byte(`{"id":"` + commandID + `","status":"pending"}`))
		case "GET /agent/v1/backups/" + commandID:
			_, _ = response.Write([]byte(`{"id":"` + commandID + `","status":"complete","summary":"Synthetic backup complete."}`))
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client := NewClient(server.URL+"/agent/v1", testBootstrap().Credential, "1.2.3-test", server.Client())

	status, err := client.StartManagedBackup(context.Background(), jobID)
	if err != nil || status.ID != commandID || status.Status != "pending" {
		t.Fatalf("queue backup: %+v %v", status, err)
	}
	status, err = client.WaitForManagedBackup(context.Background(), commandID)
	if err != nil || status.Status != "complete" || status.Summary == "" {
		t.Fatalf("wait for backup: %+v %v", status, err)
	}
}

func TestManagedBackupClientRequestsCancellationWhenWaitingIsCancelled(t *testing.T) {
	commandID := "01k4p4f7m1r9d3t6v8w2x5y7za"
	ctx, cancel := context.WithCancel(context.Background())
	var cancellations atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		switch request.Method + " " + request.URL.Path {
		case "GET /agent/v1/backups/" + commandID:
			_, _ = response.Write([]byte(`{"id":"` + commandID + `","status":"running"}`))
			cancel()
		case "POST /agent/v1/backups/" + commandID + "/cancel":
			cancellations.Add(1)
			response.WriteHeader(http.StatusNoContent)
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client := NewClient(server.URL+"/agent/v1", testBootstrap().Credential, "1.2.3-test", server.Client())

	if _, err := client.WaitForManagedBackup(ctx, commandID); err == nil {
		t.Fatal("cancelled wait succeeded")
	}
	if cancellations.Load() != 1 {
		t.Fatalf("cancellation requests: %d, want 1", cancellations.Load())
	}
}

func TestManagedBackupClientReturnsSkippedAsTerminal(t *testing.T) {
	commandID := "01k4p4f7m1r9d3t6v8w2x5y7za"
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		_, _ = response.Write([]byte(`{"id":"` + commandID + `","status":"skipped","summary":"Synthetic configuration unavailable."}`))
	}))
	defer server.Close()
	client := NewClient(server.URL+"/agent/v1", testBootstrap().Credential, "1.2.3-test", server.Client())

	status, err := client.WaitForManagedBackup(context.Background(), commandID)
	if err != nil || status.Status != "skipped" {
		t.Fatalf("wait for skipped backup: %+v %v", status, err)
	}
}
