package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestTargetedForgetCommandsRequireProtocolFifteenAndUniqueSnapshotIDs(t *testing.T) {
	command := AgentCommand{
		ID: "01k4p4h5n8d2r6t7v9w3x1yabd", Generation: 1, Kind: "run_maintenance",
		IssuedAt: "2026-09-09T08:20:00.000000Z", ExpiresAt: "2026-09-09T09:20:00.000000Z",
		Payload: CommandPayload{
			JobID: "01k4p4g2m7d9r3t6v8w1x5y2zb", RequiredConfigRevision: 4,
			Maintenance: "forget", SnapshotIDs: []string{strings.Repeat("a", 64)},
		},
	}
	if err := validateCommand(command, 1, "1.5.0"); err != nil {
		t.Fatalf("valid targeted forget: %v", err)
	}
	if err := validateCommand(command, 1, "1.4.0"); err == nil {
		t.Fatal("targeted forget was accepted before protocol 1.5")
	}
	command.Payload.SnapshotIDs = []string{strings.Repeat("a", 64), strings.Repeat("a", 64)}
	if err := validateCommand(command, 1, "1.5.0"); err == nil {
		t.Fatal("duplicate targeted snapshot identity was accepted")
	}
	command.Payload.Maintenance = "prune"
	command.Payload.SnapshotIDs = []string{strings.Repeat("b", 64)}
	if err := validateCommand(command, 1, "1.5.0"); err == nil {
		t.Fatal("targeted snapshot identities were accepted for prune")
	}
}

func TestClientNegotiatesDownAfterRollbackAndBackUpAfterUpgrade(t *testing.T) {
	var requests atomic.Int32
	var upgraded atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		attempt := requests.Add(1)
		revision := request.Header.Get(ProtocolHeader)
		if userAgent := request.Header.Get("User-Agent"); userAgent != "backupchief/1.2.3-test server/"+testBootstrap().ServerID {
			t.Fatalf("managed user agent: %q", userAgent)
		}
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
	client := NewManagedClient(server.URL, testBootstrap().Credential, "1.2.3-test", testBootstrap().ServerID, server.Client())
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

func TestHeartbeatOmitsPostgreSQLCapabilitiesForProtocolEleven(t *testing.T) {
	request := HeartbeatRequest{Capabilities: map[string]any{
		"backup_types": map[string]any{
			"file":       map[string]any{"available": true},
			"mysql":      map[string]any{"available": true},
			"postgresql": map[string]any{"available": true},
		},
		"tools": map[string]any{"mysql": map[string]any{}, "psql": map[string]any{}, "pg_dump": map[string]any{}, "snapshot_restore": map[string]any{"available": true}},
	}}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	rewritten, err := requestBodyForProtocol("/heartbeat", body, "1.1.0")
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(rewritten, &payload); err != nil {
		t.Fatal(err)
	}
	capabilities := payload["capabilities"].(map[string]any)
	backupTypes := capabilities["backup_types"].(map[string]any)
	tools := capabilities["tools"].(map[string]any)
	if backupTypes["mysql"] == nil || backupTypes["postgresql"] != nil || tools["psql"] != nil || tools["pg_dump"] != nil || tools["snapshot_restore"] != nil {
		t.Fatalf("downgraded capabilities: %#v", capabilities)
	}
}

func TestHeartbeatReportsSnapshotRestoreOnlyFromProtocolSeventeen(t *testing.T) {
	request := HeartbeatRequest{Capabilities: map[string]any{
		"backup_types": map[string]any{"file": map[string]any{"available": true}},
		"tools":        map[string]any{"snapshot_restore": map[string]any{"available": true}},
	}}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}

	for revision, expected := range map[string]bool{"1.6.0": false, "1.7.0": true} {
		rewritten, rewriteErr := requestBodyForProtocol("/heartbeat", body, revision)
		if rewriteErr != nil {
			t.Fatal(rewriteErr)
		}
		var payload map[string]any
		if err = json.Unmarshal(rewritten, &payload); err != nil {
			t.Fatal(err)
		}
		capabilities := payload["capabilities"].(map[string]any)
		tools := capabilities["tools"].(map[string]any)
		_, exists := tools["snapshot_restore"]
		if exists != expected {
			t.Fatalf("protocol %s snapshot restore capability: %#v", revision, capabilities)
		}
	}
}

func TestEnrollmentFallbackUsesTheServerRevisionInItsHeaderAndBody(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		attempt := requests.Add(1)
		if userAgent := request.Header.Get("User-Agent"); userAgent != "backupchief/1.2.3-test" {
			t.Fatalf("enrollment user agent: %q", userAgent)
		}
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

func TestArtifactBytesAreOmittedBelowTheIntroducingRevision(t *testing.T) {
	tests := []struct {
		name      string
		revision  string
		wantBytes bool
	}{
		{name: "protocol 1.3", revision: "1.3.0", wantBytes: true},
		{name: "protocol 1.2", revision: "1.2.0"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				response.Header().Set(ProtocolHeader, test.revision)
				var payload map[string]any
				if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
					t.Fatal(err)
				}
				artifacts, _ := payload["artifacts"].([]any)
				if len(artifacts) != 1 {
					t.Fatalf("artifacts: %#v", payload)
				}
				artifact, _ := artifacts[0].(map[string]any)
				_, hasSource := artifact["source_bytes"]
				_, hasStored := artifact["stored_bytes"]
				if hasSource != test.wantBytes || hasStored != test.wantBytes {
					t.Fatalf("artifact bytes present=%v/%v, want %v: %#v", hasSource, hasStored, test.wantBytes, artifact)
				}
				if artifact["snapshot_id"] == "" || artifact["database"] != "synthetic_app" {
					t.Fatalf("artifact identity lost: %#v", artifact)
				}
				response.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			client := NewClient(server.URL, testBootstrap().Credential, "1.2.3-test", server.Client())
			client.selectProtocolRevision(test.revision)

			source, stored := uint64(21901679), uint64(165183)
			result := CommandResult{
				Generation:  1,
				RunID:       "01k4p4k2n8d3r6t9v1w5x7yabc",
				Status:      "complete",
				ResultCode:  "success",
				SnapshotIDs: []string{strings.Repeat("a", 64)},
				Artifacts: []BackupArtifact{{
					Database:    "synthetic_app",
					Filename:    "synthetic_app.sql",
					SnapshotID:  strings.Repeat("a", 64),
					SourceBytes: &source,
					StoredBytes: &stored,
				}},
			}

			if err := client.SubmitResult(context.Background(), "01k4p4f7m1r9d3t6v8w2x5y7za", result); err != nil {
				t.Fatal(err)
			}
			if result.Artifacts[0].SourceBytes == nil || *result.Artifacts[0].SourceBytes != source {
				t.Fatalf("caller's artifacts were mutated: %+v", result.Artifacts[0])
			}
		})
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
