package agent

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestDefaultPathsSeparateManagedIdentityAndConfigurations(t *testing.T) {
	paths := DefaultPaths()

	if paths.Identity != "/etc/backupchief/identity.json" ||
		paths.StandaloneConfig != "/etc/backupchief/config.json" ||
		paths.ManagedConfig != "/var/lib/backupchief/config.json" {
		t.Fatalf("unexpected default paths: %+v", paths)
	}
}

func TestManagedIdentityRequiresTheCurrentProtocolRevision(t *testing.T) {
	store := newAgentTestStore(t)
	bootstrap := testBootstrap()
	if err := store.SaveBootstrap(bootstrap); err != nil {
		t.Fatal(err)
	}
	identity, err := os.ReadFile(store.Paths.Identity)
	if err != nil {
		t.Fatal(err)
	}
	identity = bytes.Replace(identity, []byte(ProtocolRevision), []byte("9.8.7"), 1)
	if err := os.WriteFile(store.Paths.Identity, identity, 0o640); err != nil {
		t.Fatal(err)
	}

	if _, err := store.LoadBootstrap(); err == nil || !strings.Contains(err.Error(), "protocol revision") {
		t.Fatalf("incompatible managed identity error: %v", err)
	}
}

func TestConfigFileStoresACompletePlainDocument(t *testing.T) {
	store := newAgentTestStore(t)
	bootstrap := testBootstrap()
	body := testConfigBody(1, 1, ProtocolRevision)

	metadata, err := store.SaveConfig(bootstrap, body, nil)
	if err != nil {
		t.Fatal(err)
	}
	plaintext, loadedMetadata, err := store.LoadConfig(bootstrap)
	if err != nil {
		t.Fatal(err)
	}

	if loadedMetadata != metadata {
		t.Fatalf("cache round trip changed configuration")
	}
	config, _, err := DecodeConfig(plaintext, bootstrap.Generation)
	if err != nil || config.Host.ID != bootstrap.ServerID {
		t.Fatalf("installed configuration lost its server identity: %v", err)
	}
	raw, err := os.ReadFile(store.Paths.ManagedConfig)
	if err != nil {
		t.Fatal(err)
	}
	var installed configDocument
	if err := json.Unmarshal(raw, &installed); err != nil {
		t.Fatal(err)
	}
	if installed.Metadata.Digest != metadata.Digest {
		t.Fatalf("installed configuration digest: %q, want %q", installed.Metadata.Digest, metadata.Digest)
	}
	for _, expected := range [][]byte{[]byte("protocol_revision"), []byte("jobs"), []byte(bootstrap.ServerID)} {
		if !bytes.Contains(raw, expected) {
			t.Fatalf("plain configuration omitted %q", expected)
		}
	}
	raw = bytes.Replace(raw, []byte(`"jobs": {`), []byte(`"jobs": {"broken":{"unexpected":true},`), 1)
	if err := os.WriteFile(store.Paths.ManagedConfig, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.LoadConfig(bootstrap); err == nil || !strings.Contains(err.Error(), "installed configuration payload is invalid") {
		t.Fatalf("tampered cache error: %v", err)
	}
}

func TestConfigCacheRejectsRollbackAndEqualRevisionConflictWithoutReplacement(t *testing.T) {
	store := newAgentTestStore(t)
	bootstrap := testBootstrap()
	revisionTwo := testConfigBody(1, 2, ProtocolRevision)
	metadata, err := store.SaveConfig(bootstrap, revisionTwo, nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := store.SaveConfig(bootstrap, testConfigBody(1, 1, ProtocolRevision), &metadata); !errors.Is(err, ErrConfigRollback) {
		t.Fatalf("rollback error: %v", err)
	}
	if _, err := store.SaveConfig(bootstrap, revisionTwo, &metadata); !errors.Is(err, ErrConfigUnchanged) {
		t.Fatalf("unchanged error: %v", err)
	}
	conflict := bytes.Replace(revisionTwo, []byte("08:15:00"), []byte("08:16:00"), 1)
	if _, err := store.SaveConfig(bootstrap, conflict, &metadata); !errors.Is(err, ErrConfigConflict) {
		t.Fatalf("conflict error: %v", err)
	}

	plaintext, loadedMetadata, err := store.LoadConfig(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := DecodeConfig(plaintext, bootstrap.Generation); err != nil || loadedMetadata != metadata {
		t.Fatal("a rejected configuration replaced the accepted cache")
	}
}

func TestDecodeConfigRejectsUnknownDuplicateAndIncompatibleFields(t *testing.T) {
	for name, body := range map[string][]byte{
		"unknown":          bytes.Replace(testConfigBody(1, 1, ProtocolRevision), []byte(`"jobs":{}`), []byte(`"jobs":{},"shell":"synthetic"`), 1),
		"duplicate":        bytes.Replace(testConfigBody(1, 1, ProtocolRevision), []byte(`"revision":1`), []byte(`"revision":1,"revision":1`), 1),
		"missing job type": bytes.Replace(validJobConfigBody(), []byte(`"type":"file",`), nil, 1),
		"unknown job type": bytes.Replace(validJobConfigBody(), []byte(`"type":"file"`), []byte(`"type":"database"`), 1),
		"host id prefix":   bytes.Replace(validJobConfigBody(), []byte(`"server_`), []byte(`"`), 1),
		"job key prefix":   bytes.Replace(validJobConfigBody(), []byte(`"job_`), []byte(`"`), 1),
		"storage prefix":   bytes.Replace(validJobConfigBody(), []byte(`"storage_`), []byte(`"`), 1),
		"generation":       testConfigBody(2, 1, ProtocolRevision),
		"protocol":         testConfigBody(1, 1, "9.9.9"),
		"schema version":   bytes.Replace(testConfigBody(1, 1, ProtocolRevision), []byte(`"schema_version":1`), []byte(`"schema_version":0`), 1),
		"null job list":    bytes.Replace(testConfigBody(1, 1, ProtocolRevision), []byte(`"jobs":{}`), []byte(`"jobs":null`), 1),
		"recent limit":     bytes.Replace(validJobConfigBody(), []byte(`"last":12`), []byte(`"last":8761`), 1),
		"service password": bytes.Replace(validJobConfigBody(), []byte(`"password":"synthetic-service-password"`), []byte(`"password":""`), 1),
		"invalid UTF-8":    invalidUTF8ConfigBody(),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := DecodeConfig(body, 1); err == nil {
				t.Fatal("invalid configuration was accepted")
			}
		})
	}
}

func TestDecodeConfigAcceptsCompleteJob(t *testing.T) {
	config, metadata, err := DecodeConfig(validJobConfigBody(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(config.Jobs) != 1 || config.Jobs[0].Type != JobTypeFile || config.Jobs[0].Key != "job_01k4p4f7m1r9d3t6v8w2x5y7za" || config.Jobs[0].ID != "01k4p4f7m1r9d3t6v8w2x5y7za" || config.Jobs[0].Enabled || metadata.Revision != 2 {
		t.Fatalf("unexpected config: %+v %+v", config, metadata)
	}
}

func TestConfigNormalizesPrefixedRunIdentity(t *testing.T) {
	body := bytes.Replace(
		validJobConfigBody(),
		[]byte(`"latest_complete":null`),
		[]byte(`"latest_complete":{"run_id":"run_01k4p4f7m1r9d3t6v8w2x5y7zb","finished_at":"2026-09-09T08:14:00.000000Z","snapshot_ids":["f83b0bcc4cb36ca077469673c1663206fd69bc9d738116d4035f76ab197c1202"]}`),
		1,
	)
	config, _, err := DecodeConfig(body, 1)
	if err != nil {
		t.Fatal(err)
	}
	if config.Jobs[0].Retention.LatestComplete.RunID != "01k4p4f7m1r9d3t6v8w2x5y7zb" {
		t.Fatalf("unexpected normalized run id: %q", config.Jobs[0].Retention.LatestComplete.RunID)
	}

	encoded, err := encodeConfig(config, strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"run_id": "run_01k4p4f7m1r9d3t6v8w2x5y7zb"`)) {
		t.Fatalf("encoded configuration did not restore the run prefix: %s", encoded)
	}
}

func TestUpdateConfigReplacesAnInvalidCacheWithoutAnEnrollmentToken(t *testing.T) {
	store := newAgentTestStore(t)
	bootstrap := testBootstrap()
	legacy := bytes.Replace(validJobConfigBody(), []byte(`"revision":2`), []byte(`"revision":1`), 1)
	legacy = bytes.Replace(legacy, []byte(`"schema_version":1`), []byte(`"schema_version":0`), 1)
	latest := validJobConfigBody()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/agent/v1/config" || request.Header.Get("Authorization") != "Bearer "+bootstrap.Credential {
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		if request.Header.Get("If-None-Match") != "" {
			t.Errorf("invalid cache ETag was sent: %q", request.Header.Get("If-None-Match"))
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		response.Header().Set("ETag", `"`+digestBody(latest)+`"`)
		_, _ = response.Write(latest)
	}))
	defer server.Close()
	bootstrap.Endpoint = server.URL + "/agent/v1"
	if err := store.SaveBootstrap(bootstrap); err != nil {
		t.Fatal(err)
	}
	writeLegacyConfigCache(t, store, bootstrap, legacy)

	result, err := UpdateConfig(context.Background(), ConfigUpdateOptions{
		Store: store, Version: "1.2.3-test", HTTPClient: server.Client(),
	})

	if err != nil {
		t.Fatal(err)
	}
	if !result.Updated || result.Metadata.Revision != 2 {
		t.Fatalf("update result: %+v", result)
	}
	_, metadata, err := store.LoadConfig(bootstrap)
	if err != nil || metadata.Revision != 2 {
		t.Fatalf("updated cache: %+v %v", metadata, err)
	}
}

func TestForcedConfigUpdateBypassesTheCurrentETag(t *testing.T) {
	store := newAgentTestStore(t)
	bootstrap := testBootstrap()
	body := testConfigBody(1, 1, ProtocolRevision)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("If-None-Match") != "" {
			t.Errorf("forced update sent an ETag: %q", request.Header.Get("If-None-Match"))
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		response.Header().Set("ETag", `"`+digestBody(body)+`"`)
		_, _ = response.Write(body)
	}))
	defer server.Close()
	bootstrap.Endpoint = server.URL + "/agent/v1"
	if err := store.SaveBootstrap(bootstrap); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SaveConfig(bootstrap, body, nil); err != nil {
		t.Fatal(err)
	}

	result, err := UpdateConfig(context.Background(), ConfigUpdateOptions{
		Store: store, Version: "1.2.3-test", HTTPClient: server.Client(), Force: true,
	})

	if err != nil {
		t.Fatal(err)
	}
	if result.Updated || result.Metadata.Revision != 1 {
		t.Fatalf("forced update result: %+v", result)
	}
}

func TestConfigUpdateRejectsAnInstalledCacheDigestFromTheControlPlane(t *testing.T) {
	body := bytes.Replace(
		testConfigBody(1, 1, ProtocolRevision),
		[]byte(`"issued_at":"2026-09-09T08:15:00.000000Z"`),
		[]byte(`"issued_at":"2026-09-09T08:15:00.000000Z","digest":"`+strings.Repeat("a", 64)+`"`),
		1,
	)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set(ProtocolHeader, ProtocolRevision)
		response.Header().Set("ETag", `"`+digestBody(body)+`"`)
		_, _ = response.Write(body)
	}))
	defer server.Close()

	client := NewClient(server.URL, testBootstrap().Credential, "1.2.3-test", server.Client())
	if _, err := client.FetchConfig(context.Background(), "", 1); err == nil || !strings.Contains(err.Error(), "installed-cache digest") {
		t.Fatalf("unexpected configuration response result: %v", err)
	}
}

func TestRepositoryValidationReportsTheInvalidFieldGroup(t *testing.T) {
	legacy := bytes.Replace(
		validJobConfigBody(),
		[]byte(`"password":"synthetic-service-password"`),
		[]byte(`"password":""`),
		1,
	)

	_, _, err := DecodeConfig(legacy, 1)

	if err == nil || !strings.Contains(err.Error(), "repository password is invalid") {
		t.Fatalf("repository error: %v", err)
	}
}

func writeLegacyConfigCache(t *testing.T, store *FileStore, bootstrap Bootstrap, body []byte) {
	t.Helper()
	if err := writeAtomic(store.Paths.ManagedConfig, body, 0o600, -1, -1); err != nil {
		t.Fatal(err)
	}
}

func validJobConfigBody() []byte {
	return []byte(`{"$schema":"https://pkg.backup.chief.app/config.schema.json","metadata":{"protocol_revision":"1.0.0","generation":1,"revision":2,"schema_version":1,"issued_at":"2026-09-09T08:15:00.000000Z"},"host":{"name":"synthetic-host","id":"server_01k4p4f7m1r9d3t6v8w2x5y7za"},"destinations":{"storage_01k4p4f7m1r9d3t6v8w2x5y7zc":{"driver":"s3","endpoint":"https://objects.example.test","region":"auto","bucket":"bucket-synthetic","prefix":"backups","access_key":"synthetic-access-key","secret_key":"synthetic-secret-key"}},"jobs":{"job_01k4p4f7m1r9d3t6v8w2x5y7za":{"name":"Synthetic documents","type":"file","enabled":false,"source":{"root":"/srv/synthetic","one_file_system":true,"excludes":[]},"repository":{"id":"bfe1c8aaeb40359d011fdfd7028992b2cb01d1c147f5651a0fd41c30164f7c7b","destination":"storage_01k4p4f7m1r9d3t6v8w2x5y7zc","path":"documents/repository","password":"synthetic-service-password"},"schedule":"0 1 * * *","retention":{"last":12,"hourly":0,"daily":7,"weekly":4,"monthly":3,"yearly":0,"forget_schedule":"17 3 * * *","prune_schedule":"47 15 * * 0"},"integrity":{"metadata_schedule":"0 2 * * 0","data_schedule":"0 3 * * 0","data_parts":4},"safety":{"latest_complete":null,"has_unresolved_runs":false}}}}`)
}

func invalidUTF8ConfigBody() []byte {
	body := testConfigBody(1, 1, ProtocolRevision)
	position := bytes.Index(body, []byte(ProtocolRevision))
	body[position] = 0xff
	return body
}

func testBootstrap() Bootstrap {
	return Bootstrap{
		Endpoint:       "https://control.example.test/agent/v1",
		ServerID:       "01k4p4f7m1r9d3t6v8w2x5y7za",
		Generation:     1,
		Credential:     base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x23}, 32)),
		Hostname:       "synthetic-host",
		SetupTokenHash: strings.Repeat("a", 64),
	}
}

func testConfigBody(generation, revision uint64, protocol string) []byte {
	return []byte(fmt.Sprintf(
		`{"$schema":"https://pkg.backup.chief.app/config.schema.json","metadata":{"protocol_revision":%q,"generation":%d,"revision":%d,"schema_version":1,"issued_at":"2026-09-09T08:15:00.000000Z"},"host":{"name":"synthetic-host","id":"server_01k4p4f7m1r9d3t6v8w2x5y7za"},"destinations":{},"jobs":{}}`,
		protocol,
		generation,
		revision,
	))
}
