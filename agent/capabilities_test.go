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
