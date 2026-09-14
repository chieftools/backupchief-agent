package agent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestPublishedSchemaAcceptsStandaloneConfiguration(t *testing.T) {
	schema := compileConfigSchema(t)
	body := standaloneConfigBody()

	validateConfigDocument(t, schema, body)
	config, _, err := DecodeConfig(body, 0)
	if err != nil {
		t.Fatal(err)
	}
	if config.Host.Name != "" || config.Host.Key != "" || len(config.Jobs) != 3 || config.Jobs[2].Type != JobTypePostgreSQLFiltered {
		t.Fatalf("unexpected standalone configuration: %+v", config)
	}
}

func TestPublishedSchemaAcceptsInstalledManagedConfiguration(t *testing.T) {
	store := newAgentTestStore(t)
	bootstrap := testBootstrap()
	if _, err := store.SaveConfig(bootstrap, validRealtimeConfigBody(t), nil); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(store.Paths.ManagedConfig)
	if err != nil {
		t.Fatal(err)
	}

	validateConfigDocument(t, compileConfigSchema(t), body)
}

func TestPublishedSchemaAcceptsManagedTransportConfiguration(t *testing.T) {
	validateConfigDocument(t, compileConfigSchema(t), validRealtimeConfigBody(t))
}

func compileConfigSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	schema, err := compiler.Compile(filepath.Join("..", "config.schema.json"))
	if err != nil {
		t.Fatalf("compile public configuration schema: %v", err)
	}
	return schema
}

func validateConfigDocument(t *testing.T, schema *jsonschema.Schema, body []byte) {
	t.Helper()
	var document any
	if err := json.Unmarshal(body, &document); err != nil {
		t.Fatal(err)
	}
	if err := schema.Validate(document); err != nil {
		t.Fatalf("configuration rejected by public schema: %v", err)
	}
}

func standaloneConfigBody() []byte {
	return []byte(`{
  "$schema": "https://pkg.backup.chief.app/config.schema.json",
  "metadata": {"schema_version": 1},
  "destinations": {
    "storage_nearby": {"driver": "local", "path": "/srv/synthetic-repositories"}
  },
  "jobs": {
    "job_documents": {
      "name": "Synthetic documents",
      "type": "file",
      "source": {"root": "/srv/synthetic-documents"},
      "repository": {"destination": "storage_nearby", "path": "documents/repository", "password": "synthetic-repository-password"},
      "schedule": "0 2 * * *"
    },
    "job_archives": {
      "name": "Synthetic archives",
      "type": "file",
      "source": {"root": "/srv/synthetic-archives"},
      "repository": {"destination": "storage_nearby", "path": "archives/repository", "password": "another-synthetic-password"},
      "schedule": "30 3 * * *"
    },
    "job_postgresql": {
      "name": "Synthetic PostgreSQL",
      "type": "postgresql_filtered",
      "source": {
        "host": "postgresql.example.test",
        "port": 5432,
        "username": "synthetic_reader",
        "password": "synthetic-secret",
        "connection_database": "postgres",
        "selection": {"mode": "exclude", "databases": ["synthetic_scratch"]}
      },
      "repository": {"destination": "storage_nearby", "path": "postgresql/repository", "password": "synthetic-postgresql-password"},
      "schedule": "45 3 * * *"
    }
  }
}`)
}
