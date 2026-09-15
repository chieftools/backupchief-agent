package rclone

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEmbeddedBinaryIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("requires embedded rclone extraction")
	}

	state := t.TempDir()
	if err := os.Chmod(state, 0o700); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(state, ".rclone-extract-interrupted.tmp")
	if err := os.WriteFile(stale, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}

	binary, err := Binary(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("interrupted extraction retained")
	}
	command := exec.Command(binary, "version")
	command.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir()}
	output, err := command.Output()
	if err != nil || !strings.HasPrefix(string(output), "rclone v"+Version+"\n") {
		t.Fatalf("unexpected rclone version: %v %s", err, output)
	}

	if err := os.Chmod(binary, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binary, []byte("corrupt executable"), 0o500); err != nil {
		t.Fatal(err)
	}
	if _, err := Binary(context.Background(), state); err == nil {
		t.Fatal("accepted corrupt executable")
	}
	if err := os.Remove(binary); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/bin/true", binary); err != nil {
		t.Fatal(err)
	}
	if _, err := Binary(context.Background(), state); err == nil {
		t.Fatal("accepted executable symlink")
	}
}

func TestExtractionHonorsCancelledContext(t *testing.T) {
	state := t.TempDir()
	if err := os.Chmod(state, 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, err := Binary(ctx, state)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancelled extraction, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("cancelled extraction exceeded its deadline")
	}
}
