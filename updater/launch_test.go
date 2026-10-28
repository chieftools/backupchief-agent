package updater

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLockUpdaterAllowsOneHelper(t *testing.T) {
	directory := t.TempDir()

	unlock, err := lockUpdater(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockUpdater(directory); err != errUpdaterRunning {
		t.Fatalf("second lock = %v, want errUpdaterRunning", err)
	}

	unlock()
	again, err := lockUpdater(directory)
	if err != nil {
		t.Fatalf("lock after unlock: %v", err)
	}
	again()
}

func TestRunLeavesTheRequestToTheRunningHelper(t *testing.T) {
	directory := t.TempDir()
	if err := WriteRequest(directory, testRequest()); err != nil {
		t.Fatal(err)
	}
	unlock, err := lockUpdater(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()

	if err := Run(context.Background(), Options{StateDirectory: directory, ServiceUID: os.Getuid(), ServiceGID: os.Getgid()}); err != nil {
		t.Fatalf("run while another helper holds the lock: %v", err)
	}
	if _, err := os.Stat(RequestPath(directory)); err != nil {
		t.Fatalf("expected the request to stay for the running helper: %v", err)
	}
}

func TestLaunchStartsTheHelperInItsOwnSessionWithItsOutputLogged(t *testing.T) {
	if _, err := os.Stat("/proc/self/stat"); err != nil {
		t.Skip("needs /proc")
	}
	directory := t.TempDir()
	helper := filepath.Join(directory, "helper")
	script := "#!/bin/sh\nset -- $(cat /proc/$$/stat)\n[ \"$1\" = \"$6\" ] && echo \"leader $0 $*\" | cut -d' ' -f1\n"
	if err := os.WriteFile(helper, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(directory, "log", "update.log")

	if err := Launch(helper, logPath); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if output, _ := os.ReadFile(logPath); strings.TrimSpace(string(output)) == "leader" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	output, _ := os.ReadFile(logPath)
	t.Fatalf("expected the helper to lead its own session, log: %q", output)
}
