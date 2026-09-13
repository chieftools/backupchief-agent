package cmd

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/chieftools/backupchief-agent/agent"
)

func TestDecodeExportPathRequiresNormalizedAbsoluteBase64URL(t *testing.T) {
	valid := base64.RawURLEncoding.EncodeToString([]byte("/srv/synthetic records/report.txt"))
	path, err := decodeExportPath(valid)
	if err != nil || path != "/srv/synthetic records/report.txt" {
		t.Fatalf("decode: %q %v", path, err)
	}

	for _, value := range []string{
		"not-base64!",
		base64.RawURLEncoding.EncodeToString([]byte("relative/report.txt")),
		base64.RawURLEncoding.EncodeToString([]byte("/srv/../private/report.txt")),
	} {
		if _, err := decodeExportPath(value); err == nil {
			t.Fatalf("accepted invalid path %q", value)
		}
	}
}

func TestPublishSnapshotExportDoesNotReplaceExistingOutput(t *testing.T) {
	directory := t.TempDir()
	part := filepath.Join(directory, "synthetic-export.zip.part")
	output := filepath.Join(directory, "synthetic-export.zip")
	if err := os.WriteFile(part, []byte("new archive"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, []byte("existing archive"), 0600); err != nil {
		t.Fatal(err)
	}

	if err := publishSnapshotExport(part, output); err == nil {
		t.Fatal("replaced an existing export")
	}
	contents, err := os.ReadFile(output)
	if err != nil || string(contents) != "existing archive" {
		t.Fatalf("existing export changed: %q %v", contents, err)
	}
}

func TestValidateJobExportSelectionUsesConfiguredRoot(t *testing.T) {
	job := agent.Job{Type: agent.JobTypeFile, Source: agent.JobSource{Root: "/srv/synthetic"}}
	if err := validateJobExportSelection(job, "directory", "/srv/synthetic/Client Records"); err != nil {
		t.Fatal(err)
	}
	if err := validateJobExportSelection(job, "file", "/srv/synthetic-other/report.txt"); err == nil {
		t.Fatal("accepted path outside configured root")
	}
	rootJob := agent.Job{Type: agent.JobTypeFile, Source: agent.JobSource{Root: "/"}}
	if err := validateJobExportSelection(rootJob, "file", "/var/lib/synthetic/report.txt"); err != nil {
		t.Fatal(err)
	}

	database := agent.Job{Type: agent.JobTypeMySQL}
	if err := validateJobExportSelection(database, "database", "/synthetic_ledger.sql"); err != nil {
		t.Fatal(err)
	}
	if err := validateJobExportSelection(database, "directory", filepath.Join("/", "synthetic_ledger.sql")); err == nil {
		t.Fatal("accepted directory selection for database job")
	}
}
