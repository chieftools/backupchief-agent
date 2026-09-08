//go:build darwin && arm64

package restic

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestDarwinEmbeddedBinary(t *testing.T) {
	if testing.Short() {
		t.Skip("requires embedded Restic extraction")
	}

	state := t.TempDir()
	if err := os.Chmod(state, 0o700); err != nil {
		t.Fatal(err)
	}

	binary, err := Binary(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(binary) != state {
		t.Fatalf("Restic extracted outside state directory: %s", binary)
	}

	command := exec.Command(binary, "version")
	command.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir()}
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(output), "restic "+Version+" ") {
		t.Fatalf("unexpected Restic version: %s", output)
	}
}
