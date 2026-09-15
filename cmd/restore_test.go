package cmd

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/chieftools/backupchief-agent/agent"
)

func TestRestoreRequestUsesTheConfiguredSnapshotRoot(t *testing.T) {
	target := filepath.Join(t.TempDir(), "restored")
	fileJob := agent.Job{Type: agent.JobTypeFile, Source: agent.JobSource{Root: "/srv/synthetic files"}}
	request, err := restoreRequest(fileJob, "synthetic-snapshot", target, "")
	if err != nil {
		t.Fatal(err)
	}
	if request.Operation != "restore" || request.Path != "/srv/synthetic files" || request.Snapshot != "synthetic-snapshot" || request.Target != target {
		t.Fatalf("file restore request: %+v", request)
	}

	databaseRequest, err := restoreRequest(agent.Job{Type: agent.JobTypeMySQL}, "synthetic-database-snapshot", target, "")
	if err != nil {
		t.Fatal(err)
	}
	if databaseRequest.Path != "/" {
		t.Fatalf("database restore request: %+v", databaseRequest)
	}
}

func TestRestoreRequestAcceptsASelectedFileDirectory(t *testing.T) {
	job := agent.Job{Type: agent.JobTypeFile, Source: agent.JobSource{Root: "/srv/synthetic files"}}
	selectedPath := "/srv/synthetic files/Quarterly Records"
	encodedPath := base64.RawURLEncoding.EncodeToString([]byte(selectedPath))

	request, err := restoreRequest(job, "synthetic-snapshot", filepath.Join(t.TempDir(), "restored"), encodedPath)
	if err != nil || request.Path != selectedPath {
		t.Fatalf("selected directory request: %+v %v", request, err)
	}

	outsidePath := base64.RawURLEncoding.EncodeToString([]byte("/srv/unrelated"))
	if _, err = restoreRequest(job, "synthetic-snapshot", "/tmp/synthetic", outsidePath); err == nil {
		t.Fatal("accepted a selected directory outside the configured root")
	}
	if _, err = restoreRequest(agent.Job{Type: agent.JobTypeMySQL}, "synthetic-snapshot", "/tmp/synthetic", encodedPath); err == nil {
		t.Fatal("accepted a selected directory for a database job")
	}
}

func TestValidateRestoreTargetAcceptsEmptyOrMissingDirectories(t *testing.T) {
	parent := t.TempDir()
	empty := filepath.Join(parent, "empty")
	if err := os.Mkdir(empty, 0o700); err != nil {
		t.Fatal(err)
	}

	for _, target := range []string{empty, filepath.Join(parent, "missing")} {
		resolved, err := validateRestoreTarget(target)
		if err != nil || resolved != target {
			t.Fatalf("target %q: %q %v", target, resolved, err)
		}
	}
}

func TestValidateRestoreTargetRejectsUnsafeDestinations(t *testing.T) {
	parent := t.TempDir()
	nonEmpty := filepath.Join(parent, "non-empty")
	if err := os.Mkdir(nonEmpty, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nonEmpty, "existing.txt"), []byte("synthetic existing content"), 0o600); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(parent, "file")
	if err := os.WriteFile(file, []byte("synthetic file"), 0o600); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(parent, "symlink")
	if err := os.Symlink(nonEmpty, symlink); err != nil {
		t.Fatal(err)
	}

	for _, target := range []string{"", string(filepath.Separator), nonEmpty, file, symlink, filepath.Join(parent, "missing", "nested")} {
		if _, err := validateRestoreTarget(target); err == nil {
			t.Fatalf("accepted unsafe restore target %q", target)
		}
	}
}
