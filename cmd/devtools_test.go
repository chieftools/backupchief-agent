//go:build devtools

package cmd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chieftools/backupchief-agent/agent"
)

const developmentTestServerID = "01k4p4f7m1r9d3t6v8w2x5y7za"

type developmentControlPlane struct {
	t           *testing.T
	mu          sync.Mutex
	credential  string
	revision    uint64
	weakETag    bool
	heartbeats  []agent.HeartbeatRequest
	configETags []string
	cancel      context.CancelFunc
}

func (controlPlane *developmentControlPlane) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	controlPlane.t.Helper()
	response.Header().Set(agent.ProtocolHeader, agent.ProtocolRevision)
	if request.Header.Get("Accept-Encoding") != "identity" {
		controlPlane.t.Errorf("unexpected content encoding request: %q", request.Header.Get("Accept-Encoding"))
	}
	switch request.URL.Path {
	case "/agent/v1/enroll":
		controlPlane.enroll(response, request)
	case "/agent/v1/config":
		controlPlane.config(response, request)
	case "/agent/v1/heartbeat":
		controlPlane.heartbeat(response, request)
	default:
		response.WriteHeader(http.StatusNotFound)
	}
}

func (controlPlane *developmentControlPlane) enroll(response http.ResponseWriter, request *http.Request) {
	var enrollment agent.EnrollmentRequest
	if err := json.NewDecoder(request.Body).Decode(&enrollment); err != nil {
		controlPlane.t.Errorf("decode enrollment: %v", err)
		response.WriteHeader(http.StatusBadRequest)
		return
	}
	if enrollment.Platform != "linux" || enrollment.Architecture != runtime.GOARCH || enrollment.Credential == "" {
		controlPlane.t.Errorf("unexpected development identity: %+v", enrollment)
	}
	controlPlane.mu.Lock()
	controlPlane.credential = enrollment.Credential
	controlPlane.mu.Unlock()
	response.WriteHeader(http.StatusCreated)
	_, _ = fmt.Fprint(response, `{"protocol_revision":"1.0.0","server_id":"`+developmentTestServerID+`","generation":1,"enrolled_at":"2026-09-09T08:15:00.000000Z","config_revision":1}`)
}

func (controlPlane *developmentControlPlane) config(response http.ResponseWriter, request *http.Request) {
	if !controlPlane.authenticated(request) {
		response.WriteHeader(http.StatusUnauthorized)
		return
	}
	controlPlane.mu.Lock()
	revision := controlPlane.revision
	weakETag := controlPlane.weakETag
	controlPlane.configETags = append(controlPlane.configETags, request.Header.Get("If-None-Match"))
	controlPlane.mu.Unlock()
	body := developmentConfig(revision)
	digest := fmt.Sprintf("%x", sha256.Sum256(body))
	etag := `"` + digest + `"`
	if weakETag {
		etag = "W/" + etag
	}
	response.Header().Set("ETag", etag)
	if request.Header.Get("If-None-Match") == `"`+digest+`"` {
		response.WriteHeader(http.StatusNotModified)
		return
	}
	response.Header().Set("Content-Type", "application/json")
	_, _ = response.Write(body)
}

func (controlPlane *developmentControlPlane) heartbeat(response http.ResponseWriter, request *http.Request) {
	if !controlPlane.authenticated(request) {
		response.WriteHeader(http.StatusUnauthorized)
		return
	}
	var heartbeat agent.HeartbeatRequest
	if err := json.NewDecoder(request.Body).Decode(&heartbeat); err != nil {
		controlPlane.t.Errorf("decode heartbeat: %v", err)
		response.WriteHeader(http.StatusBadRequest)
		return
	}
	controlPlane.mu.Lock()
	controlPlane.heartbeats = append(controlPlane.heartbeats, heartbeat)
	cancel := controlPlane.cancel
	controlPlane.mu.Unlock()
	response.WriteHeader(http.StatusNoContent)
	if heartbeat.Config.Revision == 2 && cancel != nil {
		cancel()
	}
}

func (controlPlane *developmentControlPlane) authenticated(request *http.Request) bool {
	controlPlane.mu.Lock()
	defer controlPlane.mu.Unlock()
	return controlPlane.credential != "" && request.Header.Get("Authorization") == "Bearer "+controlPlane.credential
}

func TestDevelopmentAgentSetupAvoidsSystemPathsAndSystemd(t *testing.T) {
	controlPlane := &developmentControlPlane{t: t, revision: 1}
	server := httptest.NewServer(controlPlane)
	defer server.Close()
	stateDirectory := filepath.Join(t.TempDir(), "local-agent")
	token := "bcenr_syntheticLocalToken1234567890ABCDEFGH"

	output, diagnostics, err := executeDevelopmentCommand(
		context.Background(),
		"dev", "--state-dir", stateDirectory, "setup", token, "--endpoint", server.URL+"/agent/v1",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output, developmentTestServerID) || !strings.Contains(output, "No package or service was installed") {
		t.Fatalf("unexpected setup output: %s", output)
	}
	controlPlane.mu.Lock()
	credential := controlPlane.credential
	controlPlane.mu.Unlock()
	if containsNonEmpty(output+diagnostics, token, credential) {
		t.Fatal("development output exposed setup secrets")
	}

	store, _, err := developmentStore(stateDirectory)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := store.LoadBootstrap()
	if err != nil {
		t.Fatal(err)
	}
	_, metadata, err := store.LoadConfig(bootstrap)
	if err != nil || metadata.Revision != 1 {
		t.Fatalf("initial development config: %+v %v", metadata, err)
	}
	for _, path := range []string{store.Paths.Identity, store.Paths.ManagedConfig} {
		info, statErr := os.Stat(path)
		if statErr != nil {
			t.Fatal(statErr)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode %o, want 600", path, info.Mode().Perm())
		}
	}
	if _, statErr := os.Stat(store.Paths.Pending); !os.IsNotExist(statErr) {
		t.Fatalf("pending setup remains: %v", statErr)
	}

	status, _, err := executeDevelopmentCommand(context.Background(), "dev", "--state-dir", stateDirectory, "status")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status, "Accepted config revision: 1") || containsNonEmpty(status, bootstrap.Credential, bootstrap.CacheKey) {
		t.Fatalf("unsafe or incomplete status output: %s", status)
	}
}

func TestDevelopmentAgentRefreshesConfigAndAcknowledgesIt(t *testing.T) {
	controlPlane := &developmentControlPlane{t: t, revision: 1}
	server := httptest.NewServer(controlPlane)
	defer server.Close()
	stateDirectory := filepath.Join(t.TempDir(), "refresh-agent")
	token := "bcenr_syntheticRefreshToken1234567890ABCDEF"
	if _, _, err := executeDevelopmentCommand(
		context.Background(),
		"dev", "--state-dir", stateDirectory, "setup", token, "--endpoint", server.URL+"/agent/v1",
	); err != nil {
		t.Fatal(err)
	}

	controlPlane.mu.Lock()
	controlPlane.revision = 2
	controlPlane.mu.Unlock()
	runContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	controlPlane.mu.Lock()
	controlPlane.cancel = cancel
	controlPlane.mu.Unlock()
	output, diagnostics, err := executeDevelopmentCommand(
		runContext,
		"dev", "--state-dir", stateDirectory, "run", "--heartbeat-every", "20ms", "--config-every", "20ms",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output, "Running development agent") || !strings.Contains(diagnostics, "GET /agent/v1/config -> 200") || !strings.Contains(diagnostics, "POST /agent/v1/heartbeat -> 204") {
		t.Fatalf("missing development diagnostics:\nstdout: %s\nstderr: %s", output, diagnostics)
	}

	store, _, err := developmentStore(stateDirectory)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := store.LoadBootstrap()
	if err != nil {
		t.Fatal(err)
	}
	_, metadata, err := store.LoadConfig(bootstrap)
	if err != nil || metadata.Revision != 2 {
		t.Fatalf("refreshed development config: %+v %v", metadata, err)
	}
	controlPlane.mu.Lock()
	defer controlPlane.mu.Unlock()
	acknowledged := false
	for _, heartbeat := range controlPlane.heartbeats {
		if heartbeat.Config.Status == "accepted" && heartbeat.Config.Revision == 2 {
			acknowledged = true
		}
	}
	if !acknowledged {
		t.Fatalf("revision 2 was not acknowledged: %+v", controlPlane.heartbeats)
	}
}

func TestDevelopmentConfigUpdateForcesAValidatedCacheReplacement(t *testing.T) {
	controlPlane := &developmentControlPlane{t: t, revision: 1}
	server := httptest.NewServer(controlPlane)
	defer server.Close()
	stateDirectory := filepath.Join(t.TempDir(), "config-update-agent")
	token := "bcenr_syntheticConfigUpdateToken123456789ABCDE"
	if _, _, err := executeDevelopmentCommand(
		context.Background(),
		"dev", "--state-dir", stateDirectory, "setup", token, "--endpoint", server.URL+"/agent/v1",
	); err != nil {
		t.Fatal(err)
	}
	controlPlane.mu.Lock()
	controlPlane.revision = 2
	controlPlane.mu.Unlock()

	output, diagnostics, err := executeDevelopmentCommand(
		context.Background(),
		"dev", "--state-dir", stateDirectory, "config", "update", "--force",
	)

	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output, "Configuration updated: generation 1, revision 2") || !strings.Contains(diagnostics, "GET /agent/v1/config -> 200") {
		t.Fatalf("unexpected update output:\nstdout: %s\nstderr: %s", output, diagnostics)
	}
	controlPlane.mu.Lock()
	defer controlPlane.mu.Unlock()
	if len(controlPlane.configETags) != 2 || controlPlane.configETags[1] != "" {
		t.Fatalf("forced config request ETags: %q", controlPlane.configETags)
	}
}

func TestDevelopmentAgentRejectsNonLocalEndpoint(t *testing.T) {
	stateDirectory := filepath.Join(t.TempDir(), "rejected-agent")
	_, _, err := executeDevelopmentCommand(
		context.Background(),
		"dev", "--state-dir", stateDirectory, "setup", "bcenr_syntheticRejectedToken1234567890ABCDE", "--endpoint", "https://backup.chief.app/agent/v1",
	)
	if err == nil || !strings.Contains(err.Error(), "local") {
		t.Fatalf("unexpected endpoint error: %v", err)
	}
	if _, statErr := os.Stat(stateDirectory); !os.IsNotExist(statErr) {
		t.Fatalf("rejected setup wrote state: %v", statErr)
	}
}

func TestDevelopmentStatusReportsIncompleteSetupWithoutExposingSecrets(t *testing.T) {
	controlPlane := &developmentControlPlane{t: t, revision: 1, weakETag: true}
	server := httptest.NewServer(controlPlane)
	defer server.Close()
	stateDirectory := filepath.Join(t.TempDir(), "interrupted-agent")
	token := "bcenr_syntheticInterruptedToken123456789ABCDE"

	_, setupDiagnostics, err := executeDevelopmentCommand(
		context.Background(),
		"dev", "--state-dir", stateDirectory, "setup", token, "--endpoint", server.URL+"/agent/v1",
	)
	if err == nil || !strings.Contains(err.Error(), "rerun with the same state directory and token to resume") {
		t.Fatalf("unexpected interrupted setup error: %v", err)
	}

	output, diagnostics, err := executeDevelopmentCommand(context.Background(), "dev", "--state-dir", stateDirectory, "status")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output, "Setup complete: false") || !strings.Contains(output, "Accepted config revision: unavailable") {
		t.Fatalf("incomplete setup was not reported: %s", output)
	}
	store, _, err := developmentStore(stateDirectory)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := store.LoadBootstrap()
	if err != nil {
		t.Fatal(err)
	}
	if containsNonEmpty(output+diagnostics+setupDiagnostics, token, bootstrap.Credential, bootstrap.CacheKey) {
		t.Fatal("development status exposed setup secrets")
	}

	controlPlane.mu.Lock()
	controlPlane.weakETag = false
	controlPlane.mu.Unlock()
	if _, _, err := executeDevelopmentCommand(
		context.Background(),
		"dev", "--state-dir", stateDirectory, "setup", token, "--endpoint", server.URL+"/agent/v1",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(store.Paths.Pending); !os.IsNotExist(err) {
		t.Fatalf("pending setup remains after resume: %v", err)
	}
}

func executeDevelopmentCommand(ctx context.Context, arguments ...string) (string, string, error) {
	root := NewRootCommand("1.2.3-test")
	var output bytes.Buffer
	var diagnostics bytes.Buffer
	root.SetOut(&output)
	root.SetErr(&diagnostics)
	root.SetArgs(arguments)
	err := root.ExecuteContext(ctx)
	return output.String(), diagnostics.String(), err
}

func developmentConfig(revision uint64) []byte {
	return []byte(fmt.Sprintf(
		`{"metadata":{"protocol_revision":"1.0.0","generation":1,"revision":%d,"schema_version":1,"issued_at":"2026-09-09T08:15:00.000000Z"},"host":{"id":"server_01k4p4f7m1r9d3t6v8w2x5y7za"},"destinations":{},"jobs":{}}`,
		revision,
	))
}

func containsNonEmpty(haystack string, needles ...string) bool {
	for _, needle := range needles {
		if needle != "" && strings.Contains(haystack, needle) {
			return true
		}
	}

	return false
}
