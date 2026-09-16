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
	"reflect"
	"strings"
	"testing"
)

func TestMySQLDumpFlagsRejectManagedAndOutputChangingOptions(t *testing.T) {
	for _, flag := range []string{"--result-file=/tmp/synthetic.sql", "--skip-single-transaction", "--password=synthetic-secret", "--where=synthetic"} {
		if validMySQLFlag(flag) {
			t.Fatalf("accepted unsafe flag %q", flag)
		}
	}
	if !validMySQLFlag("--hex-blob") {
		t.Fatal("rejected safe flag")
	}
}

func TestMySQLDatabaseSelectionAllowsOneThousandDatabases(t *testing.T) {
	databases := make([]string, maximumMySQLDatabases)
	for index := range databases {
		databases[index] = fmt.Sprintf("synthetic_schema_%04d", index)
	}
	source := JobSource{MySQL: &MySQLSource{
		Host: "database.example.test", Port: 3306, Username: "synthetic_reader",
		SelectionMode: "selected", Databases: databases,
	}}

	if err := validateMySQLSource(source); err != nil {
		t.Fatalf("one thousand databases: %v", err)
	}
	source.MySQL.Databases = append(source.MySQL.Databases, "synthetic_schema_overflow")
	if err := validateMySQLSource(source); err == nil {
		t.Fatal("accepted more than one thousand databases")
	}
}

func TestMySQLTableSelectionRejectsUnrepresentableExclusions(t *testing.T) {
	source := JobSource{MySQL: &MySQLSource{
		Host: "database.example.test", Port: 3306, Username: "synthetic_reader",
		SelectionMode: "selected", Databases: []string{"synthetic_app"},
		TableSelection: &TableSelection{Mode: "exclude", Tables: []TableSelectionEntry{
			{Database: "synthetic_app", Table: "events.archive"},
		}},
	}}

	if err := validateMySQLSource(source); err == nil {
		t.Fatal("accepted an exclusion name containing a period")
	}
}

func TestPostgreSQLConfigurationRequiresProtocolRevisionAndRoundTrips(t *testing.T) {
	body := postgresqlConfigBody(t, ProtocolRevision)
	config, _, err := DecodeConfig(body, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(config.Jobs) != 1 || config.Jobs[0].Type != JobTypePostgreSQL || config.Jobs[0].Source.PostgreSQL == nil || config.Jobs[0].Source.PostgreSQL.ConnectionDatabase != "postgres" {
		t.Fatalf("PostgreSQL job: %+v", config.Jobs)
	}
	encoded, err := encodeConfig(config, strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"type": "postgresql"`)) || !bytes.Contains(encoded, []byte(`"connection_database": "postgres"`)) {
		t.Fatalf("encoded PostgreSQL configuration: %s", encoded)
	}

	legacy, _, err := DecodeConfig(postgresqlConfigBody(t, "1.1.0"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(legacy.Jobs) != 0 || len(legacy.Warnings) != 1 || !strings.Contains(legacy.Warnings[0], "requires protocol revision 1.2.0") {
		t.Fatalf("legacy PostgreSQL configuration: jobs=%+v warnings=%q", legacy.Jobs, legacy.Warnings)
	}
}

func TestReplicaConfigurationRoundTrips(t *testing.T) {
	var document map[string]any
	if err := json.Unmarshal(validJobConfigBody(), &document); err != nil {
		t.Fatal(err)
	}
	document["metadata"].(map[string]any)["protocol_revision"] = ProtocolRevision
	destinations := document["destinations"].(map[string]any)
	destinations["storage_01k4p4f7m1r9d3t6v8w2x5y7zd"] = map[string]any{"driver": "local", "path": "/srv/synthetic-replicas"}
	job := document["jobs"].(map[string]any)["job_01k4p4f7m1r9d3t6v8w2x5y7za"].(map[string]any)
	primary := job["repository"].(map[string]any)
	primary["repository_key"] = "repository_01k4p4f7m1r9d3t6v8w2x5y7ze"
	job["replicas"] = []map[string]any{{
		"repository_key": "repository_01k4p4f7m1r9d3t6v8w2x5y7zf", "id": strings.Repeat("d", 64),
		"destination": "storage_01k4p4f7m1r9d3t6v8w2x5y7zd", "path": "copies/repository",
		"password": "synthetic-service-password", "status": "active", "source": primary["repository_key"],
	}}
	job["replica_setups"] = []map[string]any{{
		"repository_key": "repository_01k4p4f7m1r9d3t6v8w2x5y7zg", "id": strings.Repeat("c", 64),
		"destination": "storage_01k4p4f7m1r9d3t6v8w2x5y7zd", "path": "pending/repository",
		"password": "synthetic-service-password", "status": "provisioning", "source": primary["repository_key"],
	}}
	job["replication"] = map[string]any{"mode": "attached", "coalesce": true, "safety_hold_seconds": float64(0)}
	body, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}

	config, _, err := DecodeConfig(body, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(config.Jobs) != 1 || len(config.Jobs[0].Replicas) != 1 || len(config.Jobs[0].ReplicaSetups) != 1 || config.Jobs[0].Replicas[0].Source != config.Jobs[0].Repository.Key {
		t.Fatalf("replicas: %+v", config.Jobs)
	}
	encoded, err := encodeConfig(config, strings.Repeat("e", 64))
	if err != nil || !bytes.Contains(encoded, []byte(`"replicas": [`)) || !bytes.Contains(encoded, []byte(`"replica_setups": [`)) || !bytes.Contains(encoded, []byte(`"safety_hold_seconds": 0`)) {
		t.Fatalf("encoded replicas: %v %s", err, encoded)
	}
}

func TestReplicaConfigurationRejectsInvalidRepositoryID(t *testing.T) {
	var document map[string]any
	if err := json.Unmarshal(validJobConfigBody(), &document); err != nil {
		t.Fatal(err)
	}
	document["metadata"].(map[string]any)["protocol_revision"] = ProtocolRevision
	destinations := document["destinations"].(map[string]any)
	destinations["storage_01k4p4f7m1r9d3t6v8w2x5y7zd"] = map[string]any{"driver": "local", "path": "/srv/synthetic-replicas"}
	job := document["jobs"].(map[string]any)["job_01k4p4f7m1r9d3t6v8w2x5y7za"].(map[string]any)
	primary := job["repository"].(map[string]any)
	primary["repository_key"] = "repository_01k4p4f7m1r9d3t6v8w2x5y7ze"
	job["replicas"] = []map[string]any{{
		"repository_key": "repository_01k4p4f7m1r9d3t6v8w2x5y7zf", "id": "not-a-repository-digest",
		"destination": "storage_01k4p4f7m1r9d3t6v8w2x5y7zd", "path": "copies/repository",
		"password": "synthetic-service-password", "status": "active", "source": primary["repository_key"],
	}}
	job["replication"] = map[string]any{"mode": "attached", "coalesce": true, "safety_hold_seconds": float64(604800)}
	body, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := DecodeConfig(body, 1); err == nil {
		t.Fatal("accepted a replica with an invalid repository ID")
	}
}

func TestFilteredDatabaseConfigurationRequiresProtocolRevisionAndSurvivesCacheRoundTrip(t *testing.T) {
	body := filteredMySQLConfigBody(t, ProtocolRevision)
	config, _, err := DecodeConfig(body, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(config.Jobs) != 1 || config.Jobs[0].Type != JobTypeMySQLFiltered || config.Jobs[0].Source.MySQL == nil || config.Jobs[0].Source.MySQL.TableSelection == nil {
		t.Fatalf("filtered MySQL job: %+v", config.Jobs)
	}
	encoded, err := encodeConfig(config, strings.Repeat("c", 64))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"type": "mysql_filtered"`)) || !bytes.Contains(encoded, []byte(`"table_selection": {`)) {
		t.Fatalf("encoded filtered configuration: %s", encoded)
	}

	legacy, _, err := DecodeConfig(filteredMySQLConfigBody(t, "1.5.0"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(legacy.Jobs) != 0 || len(legacy.Warnings) != 1 || !strings.Contains(legacy.Warnings[0], "requires protocol revision 1.6.0") {
		t.Fatalf("legacy filtered configuration: jobs=%+v warnings=%q", legacy.Jobs, legacy.Warnings)
	}
	legacyEncoded, err := encodeConfig(legacy, strings.Repeat("d", 64))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(legacyEncoded, []byte(`"type": "mysql_filtered"`)) || !bytes.Contains(legacyEncoded, []byte(`"table_selection"`)) {
		t.Fatalf("legacy cache did not preserve filtered job: %s", legacyEncoded)
	}
}

func TestDatabaseExclusionConfigurationRequiresProtocolRevisionAndRoundTrips(t *testing.T) {
	body := excludedMySQLConfigBody(t, ProtocolRevision)
	config, _, err := DecodeConfig(body, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(config.Jobs) != 1 || config.Jobs[0].Type != JobTypeMySQLFiltered || config.Jobs[0].Source.MySQL == nil || config.Jobs[0].Source.MySQL.SelectionMode != "exclude" {
		t.Fatalf("excluded MySQL job: %+v", config.Jobs)
	}
	encoded, err := encodeConfig(config, strings.Repeat("e", 64))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"type": "mysql_filtered"`)) || !bytes.Contains(encoded, []byte(`"mode": "exclude"`)) {
		t.Fatalf("encoded excluded configuration: %s", encoded)
	}

	legacy, _, err := DecodeConfig(excludedMySQLConfigBody(t, "1.5.0"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(legacy.Jobs) != 0 || len(legacy.Warnings) != 1 || !strings.Contains(legacy.Warnings[0], "requires protocol revision 1.6.0") {
		t.Fatalf("legacy excluded configuration: jobs=%+v warnings=%q", legacy.Jobs, legacy.Warnings)
	}
}

func TestDatabaseAndTableExclusionsCanBeCombined(t *testing.T) {
	var document map[string]any
	if err := json.Unmarshal(excludedMySQLConfigBody(t, ProtocolRevision), &document); err != nil {
		t.Fatal(err)
	}
	job := document["jobs"].(map[string]any)["job_01k4p4f7m1r9d3t6v8w2x5y7za"].(map[string]any)
	source := job["source"].(map[string]any)
	source["table_selection"] = map[string]any{
		"mode":   "exclude",
		"tables": []map[string]any{{"database": "synthetic_active", "table": "temporary_rows"}},
	}
	body, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}

	config, _, err := DecodeConfig(body, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(config.Jobs) != 1 || config.Jobs[0].Type != JobTypeMySQLFiltered || config.Jobs[0].Source.MySQL == nil || config.Jobs[0].Source.MySQL.TableSelection == nil {
		t.Fatalf("combined filtered MySQL job: %+v", config.Jobs)
	}
}

func TestDefaultPathsSeparateManagedIdentityAndConfigurations(t *testing.T) {
	paths := DefaultPaths()

	if paths.Identity != "/etc/backupchief/identity.json" ||
		paths.StandaloneConfig != "/etc/backupchief/config.json" ||
		paths.ManagedConfig != "/var/lib/backupchief/config.json" {
		t.Fatalf("unexpected default paths: %+v", paths)
	}
}

func TestManagedIdentityAcceptsItsProtocolMajorAndRejectsAnotherMajor(t *testing.T) {
	store := newAgentTestStore(t)
	bootstrap := testBootstrap()
	if err := store.SaveBootstrap(bootstrap); err != nil {
		t.Fatal(err)
	}
	identity, err := os.ReadFile(store.Paths.Identity)
	if err != nil {
		t.Fatal(err)
	}
	identity = bytes.Replace(identity, []byte(ProtocolRevision), []byte("1.0.0"), 1)
	if err := os.WriteFile(store.Paths.Identity, identity, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadBootstrap(); err != nil {
		t.Fatalf("load same-major identity: %v", err)
	}

	identity = bytes.Replace(identity, []byte("1.0.0"), []byte("2.0.0"), 1)
	if err := os.WriteFile(store.Paths.Identity, identity, 0o640); err != nil {
		t.Fatal(err)
	}

	if _, err := store.LoadBootstrap(); err == nil || !strings.Contains(err.Error(), "protocol revision") {
		t.Fatalf("incompatible managed identity error: %v", err)
	}
}

func TestManagedConfigurationAcceptsAnEarlierProtocolMinor(t *testing.T) {
	config, _, err := DecodeConfig(testConfigBody(1, 1, "1.0.0"), 1)
	if err != nil {
		t.Fatalf("decode same-major configuration: %v", err)
	}
	if config.ProtocolRevision != "1.0.0" {
		t.Fatalf("protocol revision: %q", config.ProtocolRevision)
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

func TestConfigCacheAcceptsAnEqualRevisionProtocolVariant(t *testing.T) {
	store := newAgentTestStore(t)
	bootstrap := testBootstrap()
	legacyBody := testConfigBody(1, 2, "1.0.0")
	legacyMetadata, err := store.SaveConfig(bootstrap, legacyBody, nil)
	if err != nil {
		t.Fatal(err)
	}

	currentMetadata, err := store.SaveConfig(bootstrap, testConfigBody(1, 2, ProtocolRevision), &legacyMetadata)
	if err != nil {
		t.Fatalf("save compatible protocol variant: %v", err)
	}
	body, _, err := store.LoadConfig(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	config, _, err := DecodeConfig(body, bootstrap.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if config.ProtocolRevision != ProtocolRevision || currentMetadata.Digest == legacyMetadata.Digest {
		t.Fatalf("protocol transition was not installed: %q", config.ProtocolRevision)
	}
}

func TestDecodeConfigRejectsDuplicateAndIncompatibleFields(t *testing.T) {
	for name, body := range map[string][]byte{
		"duplicate":        bytes.Replace(testConfigBody(1, 1, ProtocolRevision), []byte(`"revision":1`), []byte(`"revision":1,"revision":1`), 1),
		"missing job type": bytes.Replace(validJobConfigBody(), []byte(`"type":"file",`), nil, 1),
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

func TestDecodeConfigIgnoresAdditiveFieldsAndSkipsUnsupportedResources(t *testing.T) {
	body := forwardCompatibleConfigBody(t)
	config, _, err := DecodeConfig(body, 1)
	if err != nil {
		t.Fatal(err)
	}
	warnings := []string{
		`destination "storage_01k4p4f7m1r9d3t6v8w2x5y7zd" uses unsupported driver "archive-vault"; skipped`,
		`job "job_01k4p4f7m1r9d3t6v8w2x5y7zb" uses unsupported type "database-export"; skipped`,
		`job "job_01k4p4f7m1r9d3t6v8w2x5y7zc" references unsupported destination "storage_01k4p4f7m1r9d3t6v8w2x5y7zd"; skipped`,
	}
	if len(config.Destinations) != 1 || len(config.Jobs) != 1 || !reflect.DeepEqual(config.Warnings, warnings) {
		t.Fatalf("unexpected compatible configuration: destinations=%d jobs=%d warnings=%q", len(config.Destinations), len(config.Jobs), config.Warnings)
	}

	store := newAgentTestStore(t)
	bootstrap := testBootstrap()
	if _, err := store.SaveConfig(bootstrap, body, nil); err != nil {
		t.Fatal(err)
	}
	cached, _, err := store.LoadConfig(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	reloaded, _, err := DecodeConfig(cached, bootstrap.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(reloaded.Warnings, warnings) || !bytes.Contains(cached, []byte(`"archive-vault"`)) || !bytes.Contains(cached, []byte(`"database-export"`)) {
		t.Fatalf("cache did not preserve unsupported resources: warnings=%q body=%s", reloaded.Warnings, cached)
	}
}

func TestDecodeConfigAcceptsCompleteJob(t *testing.T) {
	config, metadata, err := DecodeConfig(validRealtimeConfigBody(t), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(config.Jobs) != 1 || config.Jobs[0].Type != JobTypeFile || config.Jobs[0].Key != "job_01k4p4f7m1r9d3t6v8w2x5y7za" || config.Jobs[0].ID != "01k4p4f7m1r9d3t6v8w2x5y7za" || config.Jobs[0].Enabled || metadata.Revision != 2 || config.Realtime == nil || config.Realtime.Channel != "private-agent.server_01k4p4f7m1r9d3t6v8w2x5y7za.generation_1" {
		t.Fatalf("unexpected config: %+v %+v", config, metadata)
	}
	if config.Jobs[0].Maintenance.Strategy != "after_scheduled_backup" || config.Jobs[0].Maintenance.MaxDeferralSeconds != 86400 || config.Jobs[0].Maintenance.PruneIntervalSeconds != 604800 {
		t.Fatalf("unexpected maintenance policy: %+v", config.Jobs[0].Maintenance)
	}
}

func TestDecodeConfigRejectsARealtimeChannelForAnotherGeneration(t *testing.T) {
	body := bytes.Replace(validRealtimeConfigBody(t), []byte(`generation_1`), []byte(`generation_2`), 1)

	if _, _, err := DecodeConfig(body, 1); err == nil || !strings.Contains(err.Error(), "realtime channel does not match setup") {
		t.Fatalf("unexpected realtime validation error: %v", err)
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

func TestConfigRoundTripsForeverProtectedSnapshotIdentities(t *testing.T) {
	first := strings.Repeat("1", 64)
	second := strings.Repeat("2", 64)
	body := bytes.Replace(
		validJobConfigBody(),
		[]byte(`"has_unresolved_runs":false`),
		[]byte(`"has_unresolved_runs":false,"protected_snapshot_ids":["`+first+`","`+second+`"]`),
		1,
	)
	body = bytes.Replace(body, []byte(`"protocol_revision":"1.1.0"`), []byte(`"protocol_revision":"1.5.0"`), 1)
	config, _, err := DecodeConfig(body, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(config.Jobs[0].Retention.ProtectedSnapshotIDs, []string{first, second}) {
		t.Fatalf("protected snapshot identities: %v", config.Jobs[0].Retention.ProtectedSnapshotIDs)
	}
	encoded, err := encodeConfig(config, strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"protected_snapshot_ids": [`)) {
		t.Fatalf("encoded configuration omitted protected snapshots: %s", encoded)
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
	return []byte(`{"$schema":"https://pkg.backup.chief.app/config.schema.json","metadata":{"protocol_revision":"1.1.0","generation":1,"revision":2,"schema_version":1,"issued_at":"2026-09-09T08:15:00.000000Z"},"host":{"name":"synthetic-host","id":"server_01k4p4f7m1r9d3t6v8w2x5y7za"},"destinations":{"storage_01k4p4f7m1r9d3t6v8w2x5y7zc":{"driver":"s3","endpoint":"https://objects.example.test","region":"auto","bucket":"bucket-synthetic","prefix":"backups","access_key":"synthetic-access-key","secret_key":"synthetic-secret-key"}},"jobs":{"job_01k4p4f7m1r9d3t6v8w2x5y7za":{"name":"Synthetic documents","type":"file","enabled":false,"source":{"root":"/srv/synthetic","one_file_system":true,"excludes":[]},"repository":{"id":"bfe1c8aaeb40359d011fdfd7028992b2cb01d1c147f5651a0fd41c30164f7c7b","destination":"storage_01k4p4f7m1r9d3t6v8w2x5y7zc","path":"documents/repository","password":"synthetic-service-password"},"schedule":"0 1 * * *","retention":{"last":12,"hourly":0,"daily":7,"weekly":4,"monthly":3,"yearly":0,"forget_schedule":"17 3 * * *","prune_schedule":"47 15 * * 0"},"integrity":{"metadata_schedule":"0 2 * * 0","data_schedule":"0 3 * * 0","data_parts":4},"safety":{"latest_complete":null,"has_unresolved_runs":false}}}}`)
}

func postgresqlConfigBody(t *testing.T, protocol string) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(validJobConfigBody(), &document); err != nil {
		t.Fatal(err)
	}
	document["metadata"].(map[string]any)["protocol_revision"] = protocol
	jobs := document["jobs"].(map[string]any)
	job := jobs["job_01k4p4f7m1r9d3t6v8w2x5y7za"].(map[string]any)
	job["type"] = "postgresql"
	job["source"] = map[string]any{
		"host": "postgresql.example.test", "port": 5432, "username": "synthetic_reader", "password": "synthetic-secret",
		"connection_database": "postgres", "selection": map[string]any{"mode": "selected", "databases": []string{"synthetic_app"}},
	}
	body, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func filteredMySQLConfigBody(t *testing.T, protocol string) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(validJobConfigBody(), &document); err != nil {
		t.Fatal(err)
	}
	document["metadata"].(map[string]any)["protocol_revision"] = protocol
	job := document["jobs"].(map[string]any)["job_01k4p4f7m1r9d3t6v8w2x5y7za"].(map[string]any)
	job["type"] = "mysql_filtered"
	job["source"] = map[string]any{
		"host": "mysql.example.test", "port": 3306, "username": "synthetic_reader", "password": "synthetic-secret",
		"selection": map[string]any{"mode": "selected", "databases": []string{"synthetic_app"}},
		"dump":      map[string]any{"include_routines": false, "include_events": false, "custom_flags": []string{}},
		"table_selection": map[string]any{
			"mode": "exclude", "tables": []map[string]any{{"database": "synthetic_app", "table": "transient_rows"}},
		},
	}
	body, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func excludedMySQLConfigBody(t *testing.T, protocol string) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(validJobConfigBody(), &document); err != nil {
		t.Fatal(err)
	}
	document["metadata"].(map[string]any)["protocol_revision"] = protocol
	job := document["jobs"].(map[string]any)["job_01k4p4f7m1r9d3t6v8w2x5y7za"].(map[string]any)
	job["type"] = "mysql_filtered"
	job["source"] = map[string]any{
		"host": "mysql.example.test", "port": 3306, "username": "synthetic_reader", "password": "synthetic-secret",
		"selection": map[string]any{"mode": "exclude", "databases": []string{"synthetic_scratch"}},
		"dump":      map[string]any{"include_routines": false, "include_events": false, "custom_flags": []string{}},
	}
	body, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func validRealtimeConfigBody(t *testing.T) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(validJobConfigBody(), &document); err != nil {
		t.Fatal(err)
	}
	document["metadata"].(map[string]any)["protocol_revision"] = ProtocolRevision
	document["jobs"].(map[string]any)["job_01k4p4f7m1r9d3t6v8w2x5y7za"].(map[string]any)["maintenance"] = map[string]any{
		"strategy": "after_scheduled_backup", "max_deferral_seconds": 86400, "prune_interval_seconds": 604800,
	}
	document["realtime"] = map[string]any{
		"key":       "synthetic-app-key",
		"host":      "realtime.example.test",
		"port":      443,
		"channel":   "private-agent.server_01k4p4f7m1r9d3t6v8w2x5y7za.generation_1",
		"auth_url":  "https://control.example.test/agent/v1/broadcasting/auth",
		"encrypted": true,
	}
	body, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func forwardCompatibleConfigBody(t *testing.T) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(validJobConfigBody(), &document); err != nil {
		t.Fatal(err)
	}
	document["future_settings"] = map[string]any{"mode": "synthetic"}
	destinations := document["destinations"].(map[string]any)
	destinations["storage_01k4p4f7m1r9d3t6v8w2x5y7zc"].(map[string]any)["future_region_mode"] = true
	destinations["storage_01k4p4f7m1r9d3t6v8w2x5y7zd"] = map[string]any{
		"driver": "archive-vault",
		"vault":  map[string]any{"endpoint": "https://vault.example.invalid"},
	}
	jobs := document["jobs"].(map[string]any)
	knownJob := jobs["job_01k4p4f7m1r9d3t6v8w2x5y7za"].(map[string]any)
	knownJob["future_policy"] = map[string]any{"enabled": true}
	knownJob["source"].(map[string]any)["future_scanner"] = "synthetic"
	jobs["job_01k4p4f7m1r9d3t6v8w2x5y7zb"] = map[string]any{
		"name": "Synthetic export",
		"type": "database-export",
		"database": map[string]any{
			"hostname": "database.example.invalid",
		},
	}
	jobs["job_01k4p4f7m1r9d3t6v8w2x5y7zc"] = map[string]any{
		"name":    "Synthetic archive",
		"type":    "file",
		"enabled": true,
		"source":  map[string]any{"root": "/srv/synthetic-archive", "one_file_system": true},
		"repository": map[string]any{
			"destination": "storage_01k4p4f7m1r9d3t6v8w2x5y7zd",
			"path":        "archive/repository",
			"password":    "synthetic-archive-password",
		},
		"schedule": "0 4 * * *",
	}
	body, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return body
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
