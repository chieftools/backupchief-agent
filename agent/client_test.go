package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

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
