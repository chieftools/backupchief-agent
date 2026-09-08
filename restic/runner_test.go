package restic

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func testRunner(t *testing.T) Runner {
	t.Helper()
	if testing.Short() {
		t.Skip("requires real Restic execution")
	}

	return Runner{
		State:      filepath.Join(t.TempDir(), "state"),
		AllowLocal: true,
	}
}

func testRequest(t *testing.T) Request {
	t.Helper()

	return Request{
		Version:   1,
		Operation: "init",
		Connection: Connection{
			Driver: "local",
			Path:   filepath.Join(t.TempDir(), "repository"),
		},
		Password:        "synthetic-service-password",
		TimeoutSeconds:  300,
		LockWaitSeconds: 0,
	}
}

func requireComplete(t *testing.T, runner Runner, request Request) Result {
	t.Helper()

	result := runner.Run(context.Background(), request)
	if result.Outcome != "complete" || result.ExitCode != 0 {
		t.Fatalf("%s: %+v", request.Operation, result)
	}

	return result
}

func TestIndependentRecovery(t *testing.T) {
	runner := testRunner(t)
	request := testRequest(t)

	requireComplete(t, runner, request)

	request.Operation = "key_add"
	request.NewPassword = "synthetic-recovery-password"

	requireComplete(t, runner, request)

	root := filepath.Join(t.TempDir(), "source with spaces; literal")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}

	contents := []byte("Synthetic recovery fixture\n")
	if err := os.WriteFile(filepath.Join(root, "archive.txt"), contents, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "excluded.tmp"), []byte("excluded fixture"), 0600); err != nil {
		t.Fatal(err)
	}

	request.Operation = "backup"
	request.Root = root
	request.Excludes = []string{"*.tmp"}

	requireComplete(t, runner, request)

	request.Operation = "snapshots"
	result := requireComplete(t, runner, request)

	var snapshots []struct {
		ID string `json:"id"`
	}

	if err := json.Unmarshal([]byte(result.Output), &snapshots); err != nil || len(snapshots) != 1 {
		t.Fatalf("snapshots: %v %s", err, result.Output)
	}

	request.Operation = "check"

	requireComplete(t, runner, request)

	request.Operation = "stats"
	result = requireComplete(t, runner, request)
	var statistics struct {
		TotalSize uint64 `json:"total_size"`
	}
	if err := json.Unmarshal([]byte(result.Output), &statistics); err != nil || statistics.TotalSize == 0 {
		t.Fatalf("stats: %v %s", err, result.Output)
	}

	for _, password := range []string{request.Password, request.NewPassword} {
		request.Password = password
		request.Operation = "key_verify"

		requireComplete(t, runner, request)

		// Invoke stock restic independently of the runner's restore implementation.
		binary, err := Binary(context.Background(), runner.State)
		if err != nil {
			t.Fatal(err)
		}

		target := t.TempDir()
		command := exec.Command(
			binary,
			"--repo",
			request.Connection.Path,
			"restore",
			snapshots[0].ID,
			"--target",
			target,
			"--verify",
		)
		command.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir(), "RESTIC_PASSWORD=" + password}

		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("independent restore: %v %s", err, output)
		}

		restored := filepath.Join(target, strings.TrimPrefix(root, "/"), "archive.txt")
		data, err := os.ReadFile(restored)
		if err != nil || string(data) != string(contents) {
			t.Fatalf("restored data: %v %q", err, data)
		}
		if _, err := os.Stat(filepath.Join(filepath.Dir(restored), "excluded.tmp")); !os.IsNotExist(err) {
			t.Fatal("exclude was not respected")
		}
	}

	request.Password = "synthetic-wrong-password"
	request.Operation = "key_verify"

	if result := runner.Run(context.Background(), request); result.ExitCode != 12 || result.Outcome != "failed" {
		t.Fatalf("wrong password: %+v", result)
	}

	entries, err := os.ReadDir(filepath.Join(runner.State, "work"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("abandoned work: %v %v", entries, err)
	}
}

func TestRequestBoundaries(t *testing.T) {
	for _, operation := range []string{"unlock", "self-update", "forget", "unknown"} {
		request := testRequest(t)
		request.Operation = operation

		if _, _, err := request.arguments("password", "new-password", "cache", true); err == nil {
			t.Fatalf("accepted %s", operation)
		}
	}

	request := testRequest(t)
	request.Operation = "key_remove"
	request.KeyID = "short"

	if _, _, err := request.arguments("password", "new-password", "cache", true); err == nil {
		t.Fatal("accepted a short key ID")
	}

	request.KeyID = strings.Repeat("a", 64)
	args, _, err := request.arguments("password", "new-password", "cache", true)
	if err != nil || args[len(args)-2] != "remove" || args[len(args)-1] != request.KeyID {
		t.Fatalf("key remove: %v %v", args, err)
	}

	request = testRequest(t)

	if _, _, err := request.arguments("password", "new-password", "cache", false); err == nil {
		t.Fatal("accepted production local repository")
	}

	request.Operation = "backup"
	request.Root = "/literal;$(touch never).test"
	request.Excludes = []string{"[literal]*"}

	args, _, err = request.arguments("password", "new-password", "cache", true)
	if err != nil || args[len(args)-1] != request.Root || args[len(args)-2] != "--" {
		t.Fatalf("literal arguments: %v %v", args, err)
	}
	if slices.Contains(args, "--host") {
		t.Fatalf("empty host should use Restic's default: %v", args)
	}

	request.Host = "synthetic-host.example.test"
	args, _, err = request.arguments("password", "new-password", "cache", true)
	if err != nil || !slices.Contains(args, "--host") || !slices.Contains(args, request.Host) {
		t.Fatalf("explicit host: %v %v", args, err)
	}

	request = testRequest(t)
	request.Operation = "stats"
	args, _, err = request.arguments("password", "new-password", "cache", true)
	if err != nil || !slices.Contains(args, "stats") || !slices.Contains(args, "raw-data") || !slices.Contains(args, "--json") {
		t.Fatalf("stats arguments: %v %v", args, err)
	}
}

func TestPartialBackup(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires an unprivileged process without read capabilities")
	}
	runner := testRunner(t)
	request := testRequest(t)

	requireComplete(t, runner, request)

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "readable.txt"), []byte("synthetic readable file"), 0600); err != nil {
		t.Fatal(err)
	}
	unreadable := filepath.Join(root, "unreadable.txt")
	if err := os.WriteFile(unreadable, []byte("synthetic protected file"), 0000); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(unreadable, 0600)
	request.Operation = "backup"
	request.Root = root

	result := runner.Run(context.Background(), request)
	if result.Outcome != "partial" || result.ExitCode != 3 {
		t.Fatalf("partial backup: %+v", result)
	}
}

func TestBackupSelectionProtectsStateAndNestedRepository(t *testing.T) {
	runner := testRunner(t)
	root := filepath.Join(t.TempDir(), "synthetic source with spaces")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	runner.State = filepath.Join(root, ".backupchief-state")
	repository := filepath.Join(root, "nested repository")
	request := testRequest(t)
	request.Connection.Path = repository
	requireComplete(t, runner, request)

	files := map[string]string{
		".hidden":                  "hidden synthetic data",
		"file with spaces.txt":     "spaced synthetic data",
		"nested/keep.txt":          "kept synthetic data",
		"nested/private-item.skip": "excluded synthetic data",
	}
	for name, contents := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	outside := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outside, []byte("outside synthetic data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "outside-link")); err != nil {
		t.Fatal(err)
	}

	request.Operation = "backup"
	request.Root = root
	request.Excludes = []string{"nested/*.skip"}
	request.Host = "01k4p4f7m1r9d3t6v8w2x5y7zc"
	request.Tags = []string{"backupchief-job:01k4p4f7m1r9d3t6v8w2x5y7zd", "backupchief-run:01k4p4f7m1r9d3t6v8w2x5y7ze"}
	result := requireComplete(t, runner, request)

	var summary struct {
		MessageType string `json:"message_type"`
		SnapshotID  string `json:"snapshot_id"`
	}
	for _, line := range strings.Split(result.Output, "\n") {
		var candidate struct {
			MessageType string `json:"message_type"`
			SnapshotID  string `json:"snapshot_id"`
		}
		if json.Unmarshal([]byte(line), &candidate) == nil && candidate.MessageType == "summary" {
			summary = candidate
		}
	}
	if !snapshotPattern.MatchString(summary.SnapshotID) {
		t.Fatalf("missing summary snapshot: %s", result.Output)
	}

	target := t.TempDir()
	request.Operation = "restore"
	request.Snapshot = summary.SnapshotID
	request.Target = target
	requireComplete(t, runner, request)
	restoredRoot := filepath.Join(target, strings.TrimPrefix(root, "/"))

	for _, name := range []string{".hidden", "file with spaces.txt", "nested/keep.txt"} {
		if _, err := os.Stat(filepath.Join(restoredRoot, name)); err != nil {
			t.Fatalf("expected restored %s: %v", name, err)
		}
	}
	for _, name := range []string{"nested/private-item.skip", ".backupchief-state", "nested repository"} {
		if _, err := os.Stat(filepath.Join(restoredRoot, name)); !os.IsNotExist(err) {
			t.Fatalf("protected path restored %s: %v", name, err)
		}
	}
	link := filepath.Join(restoredRoot, "outside-link")
	if info, err := os.Lstat(link); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("symlink was not restored literally: %v %+v", err, info)
	}
	if destination, err := os.Readlink(link); err != nil || destination != outside {
		t.Fatalf("symlink target changed: %q %v", destination, err)
	}
}

func TestCancellationAndOutputLimits(t *testing.T) {
	for _, testCase := range []struct {
		mode    string
		timeout time.Duration
	}{
		{mode: "wait", timeout: 200 * time.Millisecond},
		{mode: "overflow", timeout: 5 * time.Second},
	} {
		t.Run(testCase.mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), testCase.timeout)
			defer cancel()
			command := exec.Command(os.Args[0], "-test.run=^TestProcessFixture$")
			command.Env = append(os.Environ(), "BACKUPCHIEF_PROCESS_FIXTURE="+testCase.mode)

			start := time.Now()
			result := runProcess(ctx, command, Request{Password: "synthetic-secret"})

			if result.Outcome == "complete" || time.Since(start) > 12*time.Second {
				t.Fatalf("unbounded execution: %+v", result)
			}
			if testCase.mode == "overflow" {
				if result.Outcome != "failed" || !result.Truncated || result.DroppedBytes == 0 || result.Output != "" || result.Diagnostic != "restic output exceeded its limit" {
					t.Fatalf("unsafe overflow: %+v", result)
				}
			}
		})
	}
}

func TestProcessFixture(t *testing.T) {
	switch os.Getenv("BACKUPCHIEF_PROCESS_FIXTURE") {
	case "wait":
		time.Sleep(time.Minute)
		os.Exit(0)
	case "overflow":
		chunk := strings.Repeat("synthetic-secret", 4096)
		for i := 0; i < 160; i++ {
			_, _ = os.Stdout.WriteString(chunk)
		}
		os.Exit(0)
	}
}

func TestSecretRedaction(t *testing.T) {
	request := Request{
		Password: "synthetic&password",
		Connection: Connection{
			SecretKey: "synthetic/storage+secret",
		},
	}

	got := redact("synthetic&password synthetic/storage+secret synthetic%26password", request)

	if got != "[REDACTED] [REDACTED] [REDACTED]" {
		t.Fatal(got)
	}
}

func TestStaleWorkspaceCleanup(t *testing.T) {
	state := t.TempDir()
	if err := os.Chmod(state, 0700); err != nil {
		t.Fatal(err)
	}

	active, cleanup, err := workspace(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	stale := filepath.Join(state, "work", "run-abandoned")
	if err := os.Mkdir(stale, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "password"), []byte("synthetic"), 0600); err != nil {
		t.Fatal(err)
	}

	_, cleanup2, err := workspace(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup2()

	if _, err := os.Stat(active); err != nil {
		t.Fatal("removed active work", err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("retained abandoned work")
	}
}
