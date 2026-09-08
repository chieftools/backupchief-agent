package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDaemonPersistsRevocationAndRefusesRestart(t *testing.T) {
	store := newAgentTestStore(t)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		writeProblem(response, http.StatusGone, "enrollment_revoked")
	}))
	defer server.Close()
	prepareDaemonStore(t, store, server.URL+"/agent/v1")

	err := Run(context.Background(), RunOptions{
		Store: store, HTTPClient: server.Client(), Version: "1.0.0",
		HeartbeatEvery: time.Millisecond, ConfigEvery: time.Millisecond,
		Jitter: func(time.Duration) time.Duration { return time.Millisecond },
	})

	if !errors.Is(err, ErrPermanentlyStopped) {
		t.Fatalf("revocation error: %v", err)
	}
	state, err := store.LoadRuntimeState()
	if err != nil {
		t.Fatal(err)
	}
	if !state.Revoked {
		t.Fatal("revocation was not persisted")
	}
	if err := Run(context.Background(), RunOptions{Store: store, HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("restart contacted the control plane")
		return nil, nil
	})}}); !errors.Is(err, ErrPermanentlyStopped) {
		t.Fatalf("restart error: %v", err)
	}
}

func TestDaemonPreservesCacheAndPausesAfter401(t *testing.T) {
	store := newAgentTestStore(t)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		writeProblem(response, http.StatusUnauthorized, "invalid_authentication")
	}))
	defer server.Close()
	prepareDaemonStore(t, store, server.URL+"/agent/v1")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := Run(ctx, RunOptions{
		Store: store, HTTPClient: server.Client(), Version: "1.0.0",
		HeartbeatEvery: time.Millisecond, ConfigEvery: time.Millisecond,
		Jitter: func(time.Duration) time.Duration { return time.Millisecond },
	})

	if err != nil {
		t.Fatal(err)
	}
	state, err := store.LoadRuntimeState()
	if err != nil {
		t.Fatal(err)
	}
	if !state.AuthenticationPaused || state.Revoked {
		t.Fatalf("unexpected runtime state: %+v", state)
	}
	bootstrap, _ := store.LoadBootstrap()
	if _, metadata, err := store.LoadConfig(bootstrap); err != nil || metadata.Revision != 1 {
		t.Fatalf("accepted cache was not preserved: %+v %v", metadata, err)
	}
}

func TestDaemonReportsRejectedConfigWithoutReplacingAcceptedCache(t *testing.T) {
	store := newAgentTestStore(t)
	var mu sync.Mutex
	var heartbeats []HeartbeatRequest
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		switch request.URL.Path {
		case "/agent/v1/config":
			body := testConfigBody(1, 2, "9.9.9")
			response.Header().Set("ETag", `"`+digestBody(body)+`"`)
			_, _ = response.Write(body)
		case "/agent/v1/heartbeat":
			var heartbeat HeartbeatRequest
			if err := json.NewDecoder(request.Body).Decode(&heartbeat); err != nil {
				t.Errorf("decode heartbeat: %v", err)
			}
			mu.Lock()
			heartbeats = append(heartbeats, heartbeat)
			mu.Unlock()
			response.WriteHeader(http.StatusNoContent)
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	prepareDaemonStore(t, store, server.URL+"/agent/v1")
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	err := Run(ctx, RunOptions{
		Store: store, HTTPClient: server.Client(), Version: "1.0.0",
		HeartbeatEvery: time.Millisecond, ConfigEvery: time.Millisecond,
		Jitter: func(time.Duration) time.Duration { return time.Millisecond },
	})

	if err != nil {
		t.Fatal(err)
	}
	bootstrap, _ := store.LoadBootstrap()
	_, metadata, err := store.LoadConfig(bootstrap)
	if err != nil || metadata.Revision != 1 {
		t.Fatalf("accepted cache changed: %+v %v", metadata, err)
	}
	mu.Lock()
	defer mu.Unlock()
	foundRejection := false
	for _, heartbeat := range heartbeats {
		if heartbeat.Config.Status == "rejected" && heartbeat.Config.Revision == 2 && heartbeat.Config.Error != "" {
			foundRejection = true
		}
	}
	if !foundRejection {
		t.Fatalf("no rejected revision heartbeat: %+v", heartbeats)
	}
}

func TestHeartbeatReportsAcceptedConfigurationWarnings(t *testing.T) {
	store := newAgentTestStore(t)
	bootstrap := testBootstrap()
	warning := `job "job_01k4p4f7m1r9d3t6v8w2x5y7zb" uses unsupported type "database-export"; skipped`
	var heartbeat HeartbeatRequest
	client := NewClient(bootstrap.Endpoint, bootstrap.Credential, "1.0.0", &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/agent/v1/heartbeat" {
			t.Fatalf("unexpected request path: %s", request.URL.Path)
		}
		if err := json.NewDecoder(request.Body).Decode(&heartbeat); err != nil {
			t.Fatal(err)
		}
		header := make(http.Header)
		header.Set(ProtocolHeader, ProtocolRevision)
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     header,
			Body:       http.NoBody,
		}, nil
	})})
	runtime := &daemon{
		store: store, client: client, bootstrap: bootstrap,
		bootID: "01k4p4f7m1r9d3t6v8w2x5y7zc",
		now:    func() time.Time { return time.Date(2026, 9, 9, 8, 15, 0, 0, time.UTC) },
		metadata: ConfigMetadata{
			Generation: 1,
			Revision:   2,
			Digest:     strings.Repeat("a", 64),
		},
		config: Config{Warnings: []string{warning}},
		state:  RuntimeState{},
	}

	if err := runtime.sendHeartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	if heartbeat.Config.Status != "accepted" || len(heartbeat.Config.Warnings) != 1 || heartbeat.Config.Warnings[0] != warning {
		t.Fatalf("unexpected heartbeat config: %+v", heartbeat.Config)
	}
}

func TestDaemonRetainsAcceptedConfigWhileControlPlaneIsUnreachable(t *testing.T) {
	store := newAgentTestStore(t)
	prepareDaemonStore(t, store, "http://127.0.0.1:1/agent/v1")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := Run(ctx, RunOptions{
		Store: store, HTTPClient: &http.Client{Timeout: 5 * time.Millisecond}, Version: "1.0.0",
		HeartbeatEvery: time.Millisecond, ConfigEvery: time.Millisecond,
		Jitter: func(time.Duration) time.Duration { return time.Millisecond },
	})

	if err != nil {
		t.Fatal(err)
	}
	bootstrap, _ := store.LoadBootstrap()
	plaintext, metadata, err := store.LoadConfig(bootstrap)
	config, _, decodeErr := DecodeConfig(plaintext, 1)
	if err != nil || decodeErr != nil || metadata.Revision != 1 || config.Revision != 1 {
		t.Fatalf("cache changed while offline: %+v %v", metadata, err)
	}
}

func TestDaemonRefreshesAnIncompatibleConfigBeforeStarting(t *testing.T) {
	store := newAgentTestStore(t)
	bootstrap := testBootstrap()
	legacy := bytes.Replace(
		testConfigBody(1, 1, ProtocolRevision),
		[]byte(`"schema_version":1`),
		[]byte(`"schema_version":0`),
		1,
	)
	latest := testConfigBody(1, 2, ProtocolRevision)
	ctx, cancel := context.WithCancel(context.Background())
	var cancelOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		switch request.URL.Path {
		case "/agent/v1/config":
			if request.Header.Get("If-None-Match") != "" {
				response.WriteHeader(http.StatusNotModified)
				return
			}
			response.Header().Set("ETag", `"`+digestBody(latest)+`"`)
			_, _ = response.Write(latest)
			cancelOnce.Do(func() { time.AfterFunc(20*time.Millisecond, cancel) })
		case "/agent/v1/heartbeat":
			response.WriteHeader(http.StatusNoContent)
		case "/agent/v1/commands":
			_ = json.NewEncoder(response).Encode(CommandsResponse{ProtocolRevision: ProtocolRevision, Commands: []AgentCommand{}})
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	bootstrap.Endpoint = server.URL + "/agent/v1"
	if err := store.SaveBootstrap(bootstrap); err != nil {
		t.Fatal(err)
	}
	writeLegacyConfigCache(t, store, bootstrap, legacy)

	if err := Run(ctx, RunOptions{Store: store, HTTPClient: server.Client(), Version: "1.0.0-test"}); err != nil {
		t.Fatal(err)
	}
	plaintext, metadata, err := store.LoadConfig(bootstrap)
	config, _, decodeErr := DecodeConfig(plaintext, 1)
	if err != nil || decodeErr != nil || metadata.Revision != 2 || config.Revision != 2 {
		t.Fatalf("refreshed cache: %+v %v", metadata, err)
	}
}

func TestDaemonReloadsValidLocalEditsAndRetainsTheLastValidConfig(t *testing.T) {
	store := newAgentTestStore(t)
	bootstrap := testBootstrap()
	initialMetadata, err := store.SaveConfig(bootstrap, testConfigBody(1, 1, ProtocolRevision), nil)
	if err != nil {
		t.Fatal(err)
	}
	initialBody, _, err := store.LoadConfig(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	initialConfig, _, err := DecodeConfig(initialBody, bootstrap.Generation)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &daemon{
		store: store, bootstrap: bootstrap, metadata: initialMetadata, config: initialConfig,
		state:   RuntimeState{Maintenance: map[string]MaintenanceRuntime{}},
		journal: CommandJournal{Version: commandJournalVersion, Commands: map[string]*JournalCommand{}},
		active:  map[string]context.CancelFunc{},
	}

	if _, err := store.SaveConfig(bootstrap, testConfigBody(1, 2, ProtocolRevision), &initialMetadata); err != nil {
		t.Fatal(err)
	}
	if err := runtime.reloadLocalConfig(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runtime.config.Revision != 2 {
		t.Fatalf("reloaded revision %d, want 2", runtime.config.Revision)
	}

	if err := os.WriteFile(store.Paths.ManagedConfig, []byte(`{"host":`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runtime.reloadLocalConfig(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runtime.config.Revision != 2 {
		t.Fatalf("invalid edit replaced revision 2 with %d", runtime.config.Revision)
	}
}

func TestDaemonUsesStandaloneConfigurationWhenManagedIdentityIsMissing(t *testing.T) {
	store := newAgentTestStore(t)
	if err := os.MkdirAll(filepath.Dir(store.Paths.StandaloneConfig), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.Paths.StandaloneConfig, standaloneConfigBody(), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := Run(ctx, RunOptions{Store: store}); err != nil {
		t.Fatal(err)
	}
}

func TestDaemonRejectsAnInvalidManagedIdentityInsteadOfUsingStandaloneConfiguration(t *testing.T) {
	store := newAgentTestStore(t)
	if err := os.MkdirAll(filepath.Dir(store.Paths.Identity), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.Paths.Identity, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.Paths.StandaloneConfig, standaloneConfigBody(), 0o600); err != nil {
		t.Fatal(err)
	}

	err := Run(context.Background(), RunOptions{Store: store})
	if err == nil || !strings.Contains(err.Error(), "load managed identity") {
		t.Fatalf("invalid managed identity result: %v", err)
	}
}

func TestAgentLoopHonorsRetryAfter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := time.Now()
	calls := 0

	err := runAgentLoop(ctx, time.Hour, 0.10, func(time.Duration) time.Duration { return 0 }, func(context.Context) error {
		calls++
		if calls == 1 {
			return &APIError{Status: http.StatusTooManyRequests, Code: "rate_limited", RetryAfter: 10 * time.Millisecond}
		}
		cancel()
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || time.Since(started) < 10*time.Millisecond {
		t.Fatalf("retry happened before Retry-After: calls=%d elapsed=%s", calls, time.Since(started))
	}
}

func TestAgentLoopRunsEarlyWhenRealtimeWakeArrives(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wake := make(chan struct{}, 1)
	calls := make(chan struct{}, 2)
	done := make(chan error, 1)

	go func() {
		done <- runTriggeredAgentLoop(ctx, time.Hour, 0, func(duration time.Duration) time.Duration { return duration }, wake, func(context.Context) error {
			calls <- struct{}{}
			return nil
		})
	}()

	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("initial agent action did not run")
	}
	notifyLoop(wake)
	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("realtime wake did not run the agent action")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func prepareDaemonStore(t *testing.T, store *FileStore, endpoint string) {
	t.Helper()
	bootstrap := testBootstrap()
	bootstrap.Endpoint = endpoint
	if err := store.SaveBootstrap(bootstrap); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SaveConfig(bootstrap, testConfigBody(1, 1, ProtocolRevision), nil); err != nil {
		t.Fatal(err)
	}
}

func writeProblem(response http.ResponseWriter, status int, code string) {
	response.Header().Set(ProtocolHeader, ProtocolRevision)
	response.Header().Set("Content-Type", "application/problem+json")
	response.WriteHeader(status)
	_, _ = io.WriteString(response, fmt.Sprintf(
		`{"type":"https://control.example.test/problems/%s","title":"Synthetic problem","status":%d,"code":%q}`,
		code,
		status,
		code,
	))
}
