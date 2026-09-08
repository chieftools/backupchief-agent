package restic

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestRepositoryContention(t *testing.T) {
	runner := testRunner(t)
	request := testRequest(t)

	requireComplete(t, runner, request)

	binary, err := Binary(context.Background(), runner.State, runner.DevelopmentBinary)
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
		return len(entries) > 0
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
	request.LockWaitSeconds = 1
	request.TimeoutSeconds = 10
	start := time.Now()
	result := runner.Run(context.Background(), request)

	if result.ExitCode != 11 || time.Since(start) > 5*time.Second {
		t.Fatalf("lock wait was not bounded: %+v", result)
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
