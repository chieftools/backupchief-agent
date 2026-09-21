package agent

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProbeExternalToolPreservesSymlinkCommandName(t *testing.T) {
	directory := t.TempDir()
	wrapper := filepath.Join(directory, "synthetic-wrapper")
	if err := os.WriteFile(wrapper, []byte(`#!/bin/sh
case "$0" in
    */synthetic-client) printf 'synthetic-client 1.2.3\n' ;;
    *) exit 1 ;;
esac
`), 0o700); err != nil {
		t.Fatal(err)
	}
	client := filepath.Join(directory, "synthetic-client")
	if err := os.Symlink(wrapper, client); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory)

	tool := probeExternalTool("synthetic-client")

	if tool["available"] != true || tool["path"] != client || tool["version"] != "synthetic-client 1.2.3" {
		t.Fatalf("tool: %#v", tool)
	}
}

func TestProbePleskVersionFileDetectsAnInstallation(t *testing.T) {
	versionFile := filepath.Join(t.TempDir(), "version")
	if err := os.WriteFile(versionFile, []byte("18.0.72 synthetic build\nignored detail\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	capability := probePleskVersionFile(versionFile)

	if capability["available"] != true || capability["version"] != "18.0.72 synthetic build" {
		t.Fatalf("Plesk capability: %#v", capability)
	}
}

func TestProbePleskVersionFileReportsAnAbsentInstallation(t *testing.T) {
	capability := probePleskVersionFile(filepath.Join(t.TempDir(), "missing"))

	if capability["available"] != false || capability["reason"] != "not_detected" {
		t.Fatalf("Plesk capability: %#v", capability)
	}
}
