package cmd

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/chieftools/backupchief-agent/agent"
	"github.com/chieftools/backupchief-agent/repository"
)

func TestDecodeSnapshotPathRequiresNormalizedAbsoluteBase64URL(t *testing.T) {
	valid := base64.RawURLEncoding.EncodeToString([]byte("/srv/synthetic records/report.txt"))
	path, err := decodeSnapshotPath(valid)
	if err != nil || path != "/srv/synthetic records/report.txt" {
		t.Fatalf("decode: %q %v", path, err)
	}

	for _, value := range []string{
		"not-base64!",
		base64.RawURLEncoding.EncodeToString([]byte("relative/report.txt")),
		base64.RawURLEncoding.EncodeToString([]byte("/srv/../private/report.txt")),
	} {
		if _, err := decodeSnapshotPath(value); err == nil {
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

func TestValidateJobSnapshotSelectionUsesConfiguredRoot(t *testing.T) {
	job := agent.Job{Type: agent.JobTypeFile, Source: agent.JobSource{Root: "/srv/synthetic"}}
	if err := validateJobSnapshotSelection(job, "directory", "/srv/synthetic/Client Records"); err != nil {
		t.Fatal(err)
	}
	if err := validateJobSnapshotSelection(job, "file", "/srv/synthetic-other/report.txt"); err == nil {
		t.Fatal("accepted path outside configured root")
	}
	rootJob := agent.Job{Type: agent.JobTypeFile, Source: agent.JobSource{Root: "/"}}
	if err := validateJobSnapshotSelection(rootJob, "file", "/var/lib/synthetic/report.txt"); err != nil {
		t.Fatal(err)
	}

	database := agent.Job{Type: agent.JobTypeMySQL}
	if err := validateJobSnapshotSelection(database, "database", "/synthetic_ledger.sql"); err != nil {
		t.Fatal(err)
	}
	if err := validateJobSnapshotSelection(database, "directory", filepath.Join("/", "synthetic_ledger.sql")); err == nil {
		t.Fatal("accepted directory selection for database job")
	}
}

func TestExportRequestUsesTheSelectedRepository(t *testing.T) {
	repository := agent.JobRepository{
		ServicePassword: "synthetic-replica-password",
		Connection:      repository.NewS3Connection(repository.S3Connection{Bucket: "replica-example-test", Prefix: "synthetic/repository"}),
	}
	request := exportRequest(
		agent.Job{Type: agent.JobTypeFile},
		repository,
		"synthetic-replica-snapshot",
		"directory",
		"/srv/synthetic",
	)

	requestS3, _ := request.Connection.S3()
	repositoryS3, _ := repository.Connection.S3()
	if request.Password != repository.ServicePassword || requestS3.Bucket != repositoryS3.Bucket || request.Snapshot != "synthetic-replica-snapshot" {
		t.Fatalf("export request: %+v", request)
	}
}
