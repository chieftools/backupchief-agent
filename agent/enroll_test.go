package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

type fakeServiceManager struct {
	mu      sync.Mutex
	calls   int
	stops   int
	err     error
	started func() error
}

func (manager *fakeServiceManager) Stop(context.Context) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.stops++
	return nil
}

func (manager *fakeServiceManager) EnableAndStart(context.Context) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.calls++
	if manager.started != nil {
		if err := manager.started(); err != nil {
			return err
		}
	}
	return manager.err
}

func (manager *fakeServiceManager) callCount() int {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return manager.calls
}

func (manager *fakeServiceManager) stopCount() int {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return manager.stops
}

func TestEnrollPersistsUsableIdentityBeforeStartingService(t *testing.T) {
	store := newAgentTestStore(t)
	token := "bcenr_syntheticEnrollmentToken1234567890ABCDEF"
	var received EnrollmentRequest
	server := enrollmentTestServer(t, &received, nil)
	defer server.Close()
	manager := &fakeServiceManager{
		started: func() error {
			bootstrap, err := store.LoadBootstrap()
			if err != nil {
				return err
			}
			_, _, err = store.LoadConfig(bootstrap)
			return err
		},
	}

	bootstrap, err := Enroll(context.Background(), token, testEnrollOptions(store, server, manager))
	if err != nil {
		t.Fatal(err)
	}

	if bootstrap.ServerID != "01k4p4f7m1r9d3t6v8w2x5y7za" || bootstrap.Generation != 1 {
		t.Fatalf("unexpected bootstrap identity: %+v", bootstrap)
	}
	if received.Credential == "" || received.AttemptID == "" || received.Platform != "linux" || received.Architecture != "amd64" {
		t.Fatalf("unexpected enrollment request: %+v", received)
	}
	if manager.stopCount() != 1 || manager.callCount() != 1 {
		t.Fatalf("service transitions: stop=%d start=%d", manager.stopCount(), manager.callCount())
	}
	var storedIdentity identityDocument
	if err := readJSON(store.Paths.Identity, &storedIdentity); err != nil {
		t.Fatal(err)
	}
	if storedIdentity.ServerID != bootstrap.ServerID || storedIdentity.Credential != bootstrap.Credential || storedIdentity.Endpoint != bootstrap.Endpoint {
		t.Fatalf("unexpected stored identity: %+v", storedIdentity)
	}
	identityInfo, err := os.Stat(store.Paths.Identity)
	if err != nil {
		t.Fatal(err)
	}
	if identityInfo.Mode().Perm() != 0o640 {
		t.Fatalf("%s mode %o, want 640", store.Paths.Identity, identityInfo.Mode().Perm())
	}
	if _, err := os.Stat(store.Paths.Pending); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pending journal remains: %v", err)
	}
	info, err := os.Stat(store.Paths.ManagedConfig)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("%s mode %o, want 600", store.Paths.ManagedConfig, info.Mode().Perm())
	}
	cache, err := os.ReadFile(store.Paths.ManagedConfig)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(cache, []byte("protocol_revision")) || !bytes.Contains(cache, []byte("destinations")) {
		t.Fatal("plain configuration is incomplete")
	}
}

func TestEnrollReusesAttemptAndCredentialAfterResponseLoss(t *testing.T) {
	store := newAgentTestStore(t)
	token := "bcenr_responseLossToken1234567890ABCDEFGHIJ"
	var requests []EnrollmentRequest
	var mu sync.Mutex
	server := enrollmentTestServer(t, nil, func(request EnrollmentRequest) int {
		mu.Lock()
		defer mu.Unlock()
		requests = append(requests, request)
		if len(requests) == 1 {
			return http.StatusCreated
		}
		return http.StatusOK
	})
	defer server.Close()
	losingClient := *server.Client()
	losingClient.Transport = &loseFirstEnrollmentResponse{base: losingClient.Transport}
	manager := &fakeServiceManager{}
	options := testEnrollOptions(store, server, manager)
	options.HTTPClient = &losingClient

	if _, err := Enroll(context.Background(), token, options); err == nil {
		t.Fatal("expected the lost response to fail the first command")
	}
	options.HTTPClient = server.Client()
	if _, err := Enroll(context.Background(), token, options); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 2 {
		t.Fatalf("enrollment requests: %d", len(requests))
	}
	if requests[0].AttemptID != requests[1].AttemptID || requests[0].Credential != requests[1].Credential {
		t.Fatalf("response-loss replay changed identity: %+v %+v", requests[0], requests[1])
	}
	if manager.callCount() != 1 {
		t.Fatalf("service start calls: %d", manager.callCount())
	}
}

func TestEnrollRetriesOnlyServiceStartAfterSystemdFailure(t *testing.T) {
	store := newAgentTestStore(t)
	token := "bcenr_serviceFailureToken1234567890ABCDEFGHI"
	var enrollments, configs int
	server := enrollmentTestServer(t, nil, func(EnrollmentRequest) int {
		enrollments++
		return http.StatusCreated
	})
	server.Config.Handler = http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		switch request.URL.Path {
		case "/agent/v1/enroll":
			enrollments++
			writeEnrollmentResponse(response, http.StatusCreated)
		case "/agent/v1/config":
			configs++
			body := testConfigBody(1, 1, ProtocolRevision)
			response.Header().Set("ETag", `"`+digestBody(body)+`"`)
			_, _ = response.Write(body)
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	})
	manager := &fakeServiceManager{err: errors.New("synthetic systemd failure")}
	options := testEnrollOptions(store, server, manager)

	if _, err := Enroll(context.Background(), token, options); err == nil {
		t.Fatal("expected systemd failure")
	}
	manager.err = nil
	if _, err := Enroll(context.Background(), token, options); err != nil {
		t.Fatal(err)
	}

	if enrollments != 1 || configs != 1 || manager.callCount() != 2 {
		t.Fatalf("unexpected retries: enroll=%d config=%d service=%d", enrollments, configs, manager.callCount())
	}
}

func TestEnrollResumesConfigurationAfterEnrollmentResponseWasPersisted(t *testing.T) {
	store := newAgentTestStore(t)
	token := "bcenr_interruptedConfigToken1234567890ABCDE"
	var enrollments, configs int
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		switch request.URL.Path {
		case "/agent/v1/enroll":
			enrollments++
			writeEnrollmentResponse(response, http.StatusCreated)
		case "/agent/v1/config":
			configs++
			body := testConfigBody(1, 1, ProtocolRevision)
			etag := `"` + digestBody(body) + `"`
			if configs == 1 {
				etag = "W/" + etag
			}
			response.Header().Set("ETag", etag)
			_, _ = response.Write(body)
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	manager := &fakeServiceManager{}
	options := testEnrollOptions(store, server, manager)

	if _, err := Enroll(context.Background(), token, options); err == nil || !strings.Contains(err.Error(), "ETag") {
		t.Fatalf("unexpected initial config error: %v", err)
	}
	if _, err := os.Stat(store.Paths.Identity); err != nil {
		t.Fatalf("bootstrap was not preserved: %v", err)
	}
	if _, err := os.Stat(store.Paths.Pending); err != nil {
		t.Fatalf("pending enrollment was not preserved: %v", err)
	}

	if _, err := Enroll(context.Background(), token, options); err != nil {
		t.Fatal(err)
	}
	if enrollments != 1 || configs != 2 || manager.callCount() != 1 {
		t.Fatalf("unexpected resume calls: enroll=%d config=%d service=%d", enrollments, configs, manager.callCount())
	}
	if _, err := os.Stat(store.Paths.Pending); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pending enrollment remains after resume: %v", err)
	}
}

func TestEnrollRejectsUnprivilegedExecutionBeforeNetworkAccess(t *testing.T) {
	store := newAgentTestStore(t)
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("network request was attempted")
		return nil, nil
	})}

	_, err := Enroll(context.Background(), "bcenr_unusedSyntheticToken1234567890ABCDEFG", EnrollOptions{
		Store: store, HTTPClient: client, RootCheck: func() bool { return false },
	})

	if err == nil || err.Error() != "setup must run as root" {
		t.Fatalf("unexpected error: %v", err)
	}
}

type loseFirstEnrollmentResponse struct {
	base http.RoundTripper
	mu   sync.Mutex
	lost bool
}

func (transport *loseFirstEnrollmentResponse) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := transport.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if !transport.lost && request.URL.Path == "/agent/v1/enroll" {
		transport.lost = true
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		return nil, io.ErrUnexpectedEOF
	}
	return response, nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func enrollmentTestServer(t *testing.T, received *EnrollmentRequest, status func(EnrollmentRequest) int) *httptest.Server {
	t.Helper()
	var credential string
	return httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		if request.Header.Get(ProtocolHeader) != ProtocolRevision {
			t.Errorf("missing protocol header")
		}
		if request.Header.Get("Accept-Encoding") != "identity" {
			t.Errorf("unexpected content encoding request: %q", request.Header.Get("Accept-Encoding"))
		}
		switch request.URL.Path {
		case "/agent/v1/enroll":
			var enrollment EnrollmentRequest
			if err := json.NewDecoder(request.Body).Decode(&enrollment); err != nil {
				t.Errorf("decode enrollment: %v", err)
			}
			credential = enrollment.Credential
			if received != nil {
				*received = enrollment
			}
			responseStatus := http.StatusCreated
			if status != nil {
				responseStatus = status(enrollment)
			}
			writeEnrollmentResponse(response, responseStatus)
		case "/agent/v1/config":
			if request.Header.Get("Authorization") != "Bearer "+credential {
				t.Errorf("config used the wrong credential")
			}
			body := testConfigBody(1, 1, ProtocolRevision)
			response.Header().Set("ETag", `"`+digestBody(body)+`"`)
			_, _ = response.Write(body)
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	}))
}

func writeEnrollmentResponse(response http.ResponseWriter, status int) {
	response.WriteHeader(status)
	_, _ = io.WriteString(response, `{"protocol_revision":"`+ProtocolRevision+`","server_id":"01k4p4f7m1r9d3t6v8w2x5y7za","generation":1,"enrolled_at":"2026-09-09T08:15:00.000000Z","config_revision":1}`)
}

func testEnrollOptions(store *FileStore, server *httptest.Server, manager ServiceManager) EnrollOptions {
	return EnrollOptions{
		Store:          store,
		Endpoint:       server.URL + "/agent/v1",
		Version:        "1.2.3-test",
		Hostname:       "synthetic-host.example.test",
		Platform:       "linux",
		Architecture:   "amd64",
		HTTPClient:     server.Client(),
		ServiceManager: manager,
		RootCheck:      func() bool { return true },
		Now:            func() time.Time { return time.Date(2026, 9, 9, 8, 15, 0, 0, time.UTC) },
	}
}

func newAgentTestStore(t *testing.T) *FileStore {
	t.Helper()
	root := t.TempDir()
	return NewTestFileStore(Paths{
		Identity:         filepath.Join(root, "etc", "identity.json"),
		Pending:          filepath.Join(root, "etc", "setup.json"),
		StandaloneConfig: filepath.Join(root, "etc", "config.json"),
		ManagedConfig:    filepath.Join(root, "state", "config.json"),
		State:            filepath.Join(root, "state", "runtime.json"),
	})
}
