package restic

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEmbeddedBinaryIntegrity(t *testing.T) {
	state := t.TempDir()
	_ = os.Chmod(state, 0700)

	stale := filepath.Join(state, ".restic-extract-interrupted.tmp")
	if err := os.WriteFile(stale, []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}

	binary, err := Binary(context.Background(), state, "")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("interrupted extraction retained")
	}
	if err := os.Chmod(binary, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binary, []byte("corrupt executable"), 0500); err != nil {
		t.Fatal(err)
	}
	if _, err := Binary(context.Background(), state, ""); err == nil {
		t.Fatal("accepted corrupt executable")
	}
	if err := os.Remove(binary); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/bin/true", binary); err != nil {
		t.Fatal(err)
	}
	if _, err := Binary(context.Background(), state, ""); err == nil {
		t.Fatal("accepted executable symlink")
	}
}

func TestRejectWrongArchitecture(t *testing.T) {
	state := t.TempDir()
	_ = os.Chmod(state, 0700)

	binary, err := Binary(context.Background(), state, "")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	data[18], data[19] = 0, 0
	path := filepath.Join(state, "wrong-architecture")

	if err := os.WriteFile(path, data, 0500); err != nil {
		t.Fatal(err)
	}
	if err := verifyFile(path, fmt.Sprintf("%x", sha256.Sum256(data))); err == nil {
		t.Fatal("accepted unsupported ELF architecture")
	}
}

func TestExtractionLockDeadline(t *testing.T) {
	state := t.TempDir()
	_ = os.Chmod(state, 0700)

	lock, err := os.OpenFile(filepath.Join(state, ".restic-extract.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()

	if err := acquire(context.Background(), lock); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	if _, err := Binary(ctx, state, ""); err == nil || time.Since(start) > time.Second {
		t.Fatalf("unbounded extraction lock: %v", err)
	}
}
