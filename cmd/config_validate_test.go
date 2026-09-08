package cmd

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chieftools/backupchief-agent/agent"
)

func TestConfigValidateUsesDownloadedConfigWhenManagedIdentityExists(t *testing.T) {
	selectedStandalonePath := filepath.Join(t.TempDir(), "config.json")
	store := agent.NewUserFileStore(pathsForConfig(selectedStandalonePath))
	identity := agent.Bootstrap{
		Endpoint:       "https://control.example.test/agent/v1",
		ServerID:       "01k4p4f7m1r9d3t6v8w2x5y7za",
		Generation:     1,
		Credential:     base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x31}, 32)),
		Hostname:       "managed-host.example.test",
		SetupTokenHash: strings.Repeat("a", 64),
	}
	if err := store.SaveBootstrap(identity); err != nil {
		t.Fatal(err)
	}
	managedConfig := []byte(`{
  "metadata":{"protocol_revision":"1.0.0","generation":1,"revision":1,"schema_version":1,"issued_at":"2026-09-12T12:00:00.000000Z"},
  "host":{"id":"server_01k4p4f7m1r9d3t6v8w2x5y7za","name":"managed-host.example.test"},
  "destinations":{"storage_01k4p4f7m1r9d3t6v8w2x5y7zc":{"driver":"local","path":"/srv/synthetic-repositories"}},
  "jobs":{"job_01k4p4f7m1r9d3t6v8w2x5y7zb":{"type":"file","source":{"root":"/srv/synthetic-records"},"repository":{"destination":"storage_01k4p4f7m1r9d3t6v8w2x5y7zc","path":"records/repository","password":"synthetic-repository-password"},"schedule":"30 4 * * *"}}
}`)
	if _, err := store.SaveConfig(identity, managedConfig, nil); err != nil {
		t.Fatal(err)
	}

	root := NewRootCommand("1.2.3-test")
	var output bytes.Buffer
	root.SetOut(&output)
	root.SetArgs([]string{"--config", selectedStandalonePath, "config", "validate"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	if output.String() != "Configuration is valid (managed, 1 jobs).\n" {
		t.Fatalf("unexpected validation output: %q", output.String())
	}
}

func TestConfigValidateChecksTheSelectedStandaloneFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent-config.json")
	body := []byte(`{
  "$schema":"https://pkg.backup.chief.app/config.schema.json",
  "metadata":{"schema_version":1},
  "future_settings":{"synthetic":true},
  "destinations":{"storage_local":{"driver":"local","path":"/srv/synthetic-repositories"}},
  "jobs":{
    "job_database_export":{"type":"database-export","database":{"hostname":"database.example.invalid"}},
    "job_media":{"type":"file","source":{"root":"/srv/synthetic-media"},"repository":{"destination":"storage_local","path":"media/repository","password":"synthetic-password"},"schedule":"15 4 * * *"}
  }
}`)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}

	root := NewRootCommand("1.2.3-test")
	var output bytes.Buffer
	root.SetOut(&output)
	root.SetArgs([]string{"--config", path, "config", "validate"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	expected := "Configuration warning: job \"job_database_export\" uses unsupported type \"database-export\"; skipped\n" +
		"Configuration is valid (standalone, 1 jobs).\n"
	if output.String() != expected {
		t.Fatalf("unexpected validation output: %q", output.String())
	}
}
