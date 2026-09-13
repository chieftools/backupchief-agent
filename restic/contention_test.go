package restic

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRepositoryContention(t *testing.T) {
	runner := testRunner(t)
	request := testRequest(t)

	requireComplete(t, runner, request)

	binary, err := Binary(context.Background(), runner.State)
	if err != nil {
		t.Fatal(err)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()

	backup := exec.Command(
		binary,
		"--repo",
		request.Connection.Path,
		"backup",
		"--stdin",
		"--stdin-filename",
		"synthetic-stream.txt",
	)
	backup.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir(), "RESTIC_PASSWORD=" + request.Password}
	backup.Stdin = reader

	if err := backup.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = writer.Close()
		_ = backup.Wait()
	}()

	waitFor(t, func() bool {
		entries, _ := os.ReadDir(filepath.Join(request.Connection.Path, "locks"))
		for _, entry := range entries {
			if snapshotPattern.MatchString(entry.Name()) {
				return true
			}
		}
		return false
	})
	contentionCases := []struct {
		operation string
		code      int
	}{
		{operation: "prune", code: 11},
		{operation: "check", code: 11},
		{operation: "check_metadata", code: 11},
		{operation: "check_data", code: 11},
		{operation: "key_add", code: 0},
	}

	for _, testCase := range contentionCases {
		request.Operation = testCase.operation
		request.NewPassword = "synthetic-overlap-key"
		request.DataSubsetPart = 1
		request.DataSubsetTotal = 2

		result := runner.Run(context.Background(), request)

		t.Logf("backup versus %s: exit %d, %s", testCase.operation, result.ExitCode, result.Outcome)
		if result.ExitCode != testCase.code {
			t.Fatalf("unexpected contention result: %+v", result)
		}
	}
	request.Operation = "prune"
	request.RecoverStaleLocks = true
	result := runner.Run(context.Background(), request)
	if result.ExitCode != 11 || result.Outcome != "locked" {
		t.Fatalf("active repository lock was removed: %+v", result)
	}

	request.Operation = "prune"
	request.RecoverStaleLocks = false
	request.LockWaitSeconds = 1
	request.TimeoutSeconds = 10
	start := time.Now()
	result = runner.Run(context.Background(), request)

	if result.ExitCode != 11 || time.Since(start) > 5*time.Second {
		t.Fatalf("lock wait was not bounded: %+v", result)
	}
}

func TestMaintenanceRecoversAStaleRepositoryLockOnce(t *testing.T) {
	runner := testRunner(t)
	request := testRequest(t)
	requireComplete(t, runner, request)

	binary, err := Binary(context.Background(), runner.State)
	if err != nil {
		t.Fatal(err)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	backup := exec.Command(
		binary,
		"--repo",
		request.Connection.Path,
		"backup",
		"--stdin",
		"--stdin-filename",
		"synthetic-interrupted-stream.txt",
	)
	backup.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir(), "RESTIC_PASSWORD=" + request.Password}
	backup.Stdin = reader
	if err = backup.Start(); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool {
		entries, _ := os.ReadDir(filepath.Join(request.Connection.Path, "locks"))
		for _, entry := range entries {
			if snapshotPattern.MatchString(entry.Name()) {
				return true
			}
		}
		return false
	})
	if err = backup.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = backup.Wait()
	_ = reader.Close()
	_ = writer.Close()

	request.Operation = "prune"
	request.TimeoutSeconds = 30
	request.LockWaitSeconds = 0
	request.RecoverStaleLocks = true
	recovered := runner.Run(context.Background(), request)
	if recovered.ExitCode != 0 || recovered.Outcome != "complete" || !strings.Contains(recovered.Diagnostic, "stale repository lock cleanup attempted") {
		t.Fatalf("stale lock recovery failed: %+v", recovered)
	}
	entries, err := os.ReadDir(filepath.Join(request.Connection.Path, "locks"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("repository locks remain after recovery: %v %v", entries, err)
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("timed out waiting for test condition")
}
