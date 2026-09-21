package restic

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/chieftools/backupchief-agent/repository"
)

func TestSFTPRepositoryUsesPinnedBundledRcloneTransport(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}

	request := testRequest(t)
	request.Connection = repository.NewSFTPConnection(repository.SFTPConnection{
		Host: "archive.example.test", Port: 2222, Username: "synthetic-backup",
		Path: "/repositories/job-example", HostKeys: []string{"ssh-ed25519 c3ludGhldGljLWhvc3Qta2V5"},
		Authentication: repository.Ed25519Authentication(string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded}))),
	})

	prepared, configuration, err := repository.PrepareRclone(request.Connection, repository.RcloneOptions{
		Name: "backupchief_repository", Program: "/private/runtime/rclone", ProxyURL: "http://127.0.0.1:43210",
	})
	if err != nil {
		t.Fatal(err)
	}

	arguments, _, err := request.argumentsPrepared("password", "", "cache", prepared)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"type = sftp",
		"host = archive.example.test",
		"port = 2222",
		"host_keys = ssh-ed25519 c3ludGhldGljLWhvc3Qta2V5",
		"http_proxy = http://127.0.0.1:43210",
		"shell_type = none",
		"disable_hashcheck = true",
	} {
		if !strings.Contains(configuration, expected) {
			t.Fatalf("missing %q in configuration: %s", expected, configuration)
		}
	}

	if !slices.Contains(arguments, "rclone.program=/private/runtime/rclone") {
		t.Fatalf("arguments=%v", arguments)
	}
}

func TestSFTPPasswordIsObscuredBeforeWritingRcloneConfiguration(t *testing.T) {
	request := testRequest(t)
	request.Connection = repository.NewSFTPConnection(repository.SFTPConnection{
		Host: "password.example.test", Port: 22, Username: "synthetic-backup",
		Path: "/repositories/job-password", HostKeys: []string{"ssh-ed25519 c3ludGhldGljLWhvc3Qta2V5"},
		Authentication: repository.PasswordAuthentication("synthetic-sftp-password"),
	})
	_, configuration, err := repository.PrepareRclone(request.Connection, repository.RcloneOptions{
		Name: "backupchief_repository", Program: "/private/runtime/rclone", ProxyURL: "http://127.0.0.1:43210",
		Obscure: func(password string) (string, error) {
			if password != "synthetic-sftp-password" {
				t.Fatalf("password: %q", password)
			}
			return "obscured-synthetic-password", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(configuration, "pass = obscured-synthetic-password") || strings.Contains(configuration, "synthetic-sftp-password") {
		t.Fatalf("configuration: %s", configuration)
	}
}

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

func TestStdinBackupBuildsACommandWithoutShellInterpolation(t *testing.T) {
	request := testRequest(t)
	request.Operation = "backup_stdin"
	request.Root = ""
	request.StdinFilename = "73796e746865746963.sql"
	request.StdinCommand = []string{"/usr/bin/mysqldump", "--defaults-extra-file={backupchief-command-config}", "--databases", "synthetic;literal"}
	request.CommandConfig = "[client]\npassword=synthetic-secret\n"
	request.Tags = []string{"backupchief-type:mysql"}

	arguments, _, err := request.arguments("password", "", "cache", true)
	if err != nil {
		t.Fatal(err)
	}
	wantTail := []string{"--", "/usr/bin/mysqldump", "--defaults-extra-file={backupchief-command-config}", "--databases", "synthetic;literal"}
	if len(arguments) < len(wantTail) || !reflect.DeepEqual(arguments[len(arguments)-len(wantTail):], wantTail) {
		t.Fatalf("arguments: %v", arguments)
	}
}

func TestResticCommandsUseLowImpactExecutionSettings(t *testing.T) {
	request := testRequest(t)
	request.Operation = "backup"
	request.Root = "/srv/synthetic-source"

	arguments, environment, err := request.arguments("password", "", "cache", true)
	if err != nil {
		t.Fatal(err)
	}

	for _, expected := range []string{"GOMAXPROCS=1", "RESTIC_READ_CONCURRENCY=1"} {
		if !slices.Contains(environment, expected) {
			t.Fatalf("missing execution setting %q: %v", expected, environment)
		}
	}
	if !slices.Contains(arguments, "--no-scan") {
		t.Fatalf("filesystem backup retains its progress scan: %v", arguments)
	}
}

func TestAWSRepositoryAndGuardUseTheDerivedEndpoint(t *testing.T) {
	request := testRequest(t)
	request.Connection = repository.NewS3Connection(repository.S3Connection{
		Endpoint: "https://s3.eu-west-3.amazonaws.com", Region: "eu-west-3", Bucket: "synthetic-archive",
		Prefix: "repositories/synthetic-job", AccessKey: "synthetic-access", SecretKey: "synthetic-secret",
	})

	arguments, _, err := request.arguments("password", "", "cache", false)
	if err != nil {
		t.Fatal(err)
	}
	target, _, err := request.Connection.Target()
	if err != nil {
		t.Fatal(err)
	}

	repository := "s3:https://s3.dualstack.eu-west-3.amazonaws.com/synthetic-archive/repositories/synthetic-job"
	if !slices.Contains(arguments, repository) {
		t.Fatalf("repository arguments: %v", arguments)
	}
	if target.Host != "s3.dualstack.eu-west-3.amazonaws.com" || target.Port != 443 {
		t.Fatalf("guarded target: %+v", target)
	}
}

func testRequest(t *testing.T) Request {
	t.Helper()

	return Request{
		Version:         1,
		Operation:       "init",
		Connection:      repository.NewLocalConnection(filepath.Join(t.TempDir(), "repository")),
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
			request.Connection.RepositoryPath(),
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

func TestInitializesAndCopiesBetweenLocalRepositories(t *testing.T) {
	runner := testRunner(t)
	source := testRequest(t)
	requireComplete(t, runner, source)

	root := filepath.Join(t.TempDir(), "synthetic source")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "record.txt"), []byte("synthetic replica payload\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	source.Operation = "backup"
	source.Root = root
	requireComplete(t, runner, source)

	destination := testRequest(t)
	destination.Operation = "init_from"
	destination.SourceConnection = &source.Connection
	destination.SourcePassword = source.Password
	requireComplete(t, runner, destination)

	destination.Operation = "copy"
	requireComplete(t, runner, destination)
	destination.Operation = "snapshots"
	destination.SourceConnection = nil
	destination.SourcePassword = ""
	result := requireComplete(t, runner, destination)

	var snapshots []struct {
		ID       string `json:"id"`
		Original string `json:"original"`
	}
	if err := json.Unmarshal([]byte(result.Output), &snapshots); err != nil || len(snapshots) != 1 || snapshots[0].ID == "" || snapshots[0].Original == "" {
		t.Fatalf("copied snapshots: %v %s", err, result.Output)
	}
}

func TestDualRepositoryTransportUsesBundledRclone(t *testing.T) {
	source := repository.NewS3Connection(repository.S3Connection{
		Endpoint: "https://source.storage.example.test", Bucket: "synthetic-source", Prefix: "repository",
		Region: "test-1", AccessKey: "synthetic-source-key", SecretKey: "synthetic-source-secret",
	})
	request := Request{
		Version: 1, Operation: "copy", Connection: repository.NewS3Connection(repository.S3Connection{
			Endpoint: "https://destination.storage.example.test", Bucket: "synthetic-destination", Prefix: "repository",
			Region: "test-2", AccessKey: "synthetic-destination-key", SecretKey: "synthetic-destination-secret",
		}),
		SourceConnection: &source, Password: "synthetic-destination-password", SourcePassword: "synthetic-source-password",
		TimeoutSeconds: 3600, LockWaitSeconds: 30,
	}

	destinationPrepared, destinationConfig, err := repository.PrepareRclone(request.Connection, repository.RcloneOptions{
		Name: "backupchief_destination", Program: "/private/runtime/rclone",
	})
	if err != nil {
		t.Fatal(err)
	}

	sourcePrepared, sourceConfig, err := repository.PrepareRclone(source, repository.RcloneOptions{
		Name: "backupchief_source", Program: "/private/runtime/rclone",
	})
	if err != nil {
		t.Fatal(err)
	}

	args, environment, err := request.dualArguments("destination-password", "source-password", "cache", destinationPrepared, sourcePrepared)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(args, "rclone.program=/private/runtime/rclone") {
		t.Fatalf("missing bundled rclone program: %v", args)
	}
	for _, expected := range []string{"GOMAXPROCS=1", "RESTIC_READ_CONCURRENCY=1"} {
		if !slices.Contains(environment, expected) {
			t.Fatalf("missing dual-repository execution setting %q: %v", expected, environment)
		}
	}

	config := sourceConfig + destinationConfig
	for _, section := range []string{"[backupchief_source]", "[backupchief_destination]"} {
		if !strings.Contains(config, section) {
			t.Fatalf("missing rclone remote %s: %s", section, config)
		}
	}
	if strings.Count(config, "no_check_bucket = true") != 2 {
		t.Fatalf("rclone remotes may attempt bucket creation: %s", config)
	}

	if _, _, err := repository.PrepareRclone(source, repository.RcloneOptions{Name: "backupchief_source"}); err == nil {
		t.Fatal("accepted a system-resolved rclone executable")
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
	request.Operation = "backup"
	request.Root = t.TempDir()
	request.RecoverStaleLocks = true
	if _, _, err := request.arguments("password", "new-password", "cache", true); err == nil {
		t.Fatal("accepted stale lock recovery for a backup")
	}

	request = testRequest(t)
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

	request.Root = "/"
	request.Paths = []string{"/srv/synthetic-sites", "/var/mail/synthetic.test"}
	args, _, err = request.arguments("password", "new-password", "cache", true)
	if err != nil || !reflect.DeepEqual(args[len(args)-2:], request.Paths) {
		t.Fatalf("multiple backup paths: %v %v", args, err)
	}

	request = testRequest(t)
	request.Operation = "stats"
	args, _, err = request.arguments("password", "new-password", "cache", true)
	if err != nil || !slices.Contains(args, "stats") || !slices.Contains(args, "raw-data") || !slices.Contains(args, "--json") {
		t.Fatalf("stats arguments: %v %v", args, err)
	}

	request = testRequest(t)
	request.Operation = "ls"
	request.Snapshot = strings.Repeat("d", 64)
	request.Path = "/srv/synthetic files"
	args, _, err = request.arguments("password", "new-password", "cache", true)
	if err != nil || !reflect.DeepEqual(args[len(args)-3:], []string{"cat", "tree", request.Snapshot + ":" + request.Path}) {
		t.Fatalf("ls arguments: %v %v", args, err)
	}

	for _, path := range []string{"", "relative", "/srv/../private", "/srv//nested", "/srv/trailing/", "/srv\\windows"} {
		request.Path = path
		if _, _, err := request.arguments("password", "new-password", "cache", true); err == nil {
			t.Fatalf("accepted unsafe snapshot path %q", path)
		}
	}

	request = testRequest(t)
	request.Operation = "restore"
	request.Snapshot = strings.Repeat("e", 64)
	request.Path = "/srv/synthetic restore"
	request.Target = "/tmp/synthetic restore target"
	args, _, err = request.arguments("password", "new-password", "cache", true)
	wantRestore := []string{"restore", request.Snapshot + ":" + request.Path, "--target", request.Target, "--verify", "--overwrite", "never"}
	if err != nil || !reflect.DeepEqual(args[len(args)-len(wantRestore):], wantRestore) {
		t.Fatalf("restore arguments: %v %v", args, err)
	}
}

func TestDirectoryListingIsNonRecursive(t *testing.T) {
	runner := testRunner(t)
	request := testRequest(t)
	requireComplete(t, runner, request)

	root := filepath.Join(t.TempDir(), "synthetic tree")
	for name, contents := range map[string]string{
		"visible.txt":           "visible synthetic file",
		"nested/child.txt":      "nested synthetic file",
		"nested/deeper/end.txt": "deep synthetic file",
	} {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("nested", filepath.Join(root, "current")); err != nil {
		t.Fatal(err)
	}

	request.Operation = "backup"
	request.Root = root
	result := requireComplete(t, runner, request)

	var snapshotID string
	for _, line := range strings.Split(result.Output, "\n") {
		var summary struct {
			MessageType string `json:"message_type"`
			SnapshotID  string `json:"snapshot_id"`
		}
		if json.Unmarshal([]byte(line), &summary) == nil && summary.MessageType == "summary" {
			snapshotID = summary.SnapshotID
		}
	}

	request.Operation = "ls"
	request.Snapshot = snapshotID
	request.Path = root
	result = requireComplete(t, runner, request)

	type listedNode struct {
		Name       string `json:"name"`
		Type       string `json:"type"`
		LinkTarget string `json:"linktarget"`
	}
	var tree struct {
		Nodes []listedNode `json:"nodes"`
	}
	if err := json.Unmarshal([]byte(result.Output), &tree); err != nil {
		t.Fatalf("directory listing: %v %s", err, result.Output)
	}

	nodes := make(map[string]listedNode, len(tree.Nodes))
	for _, node := range tree.Nodes {
		nodes[node.Name] = node
	}

	if _, exists := nodes["visible.txt"]; !exists {
		t.Fatalf("file missing from directory children: %v", nodes)
	}
	if _, exists := nodes["nested"]; !exists {
		t.Fatalf("folder missing from directory children: %v", nodes)
	}
	if _, exists := nodes["child.txt"]; exists {
		t.Fatalf("directory listing was recursive: %v", nodes)
	}
	if current := nodes["current"]; current.Type != "symlink" || current.LinkTarget != "nested" {
		t.Fatalf("symlink target missing from directory listing: %v", current)
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
	repositoryPath := filepath.Join(root, "nested repository")
	request := testRequest(t)
	request.Connection = repository.NewLocalConnection(repositoryPath)
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
	request.Path = root
	requireComplete(t, runner, request)
	restoredRoot := target

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

func TestDirectoryListingUsesFourMegabyteOutputLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.Command(os.Args[0], "-test.run=^TestProcessFixture$")
	command.Env = append(os.Environ(), "BACKUPCHIEF_PROCESS_FIXTURE=browse-overflow")

	result := runProcess(ctx, command, Request{Operation: "ls", Password: "synthetic-secret"})

	if result.Outcome != "failed" || !result.Truncated || result.DroppedBytes == 0 || result.Output != "" || result.Diagnostic != "restic output exceeded its limit" {
		t.Fatalf("unsafe directory listing overflow: %+v", result)
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
	case "browse-overflow":
		chunk := strings.Repeat("x", 1<<20)
		for i := 0; i < 5; i++ {
			_, _ = os.Stdout.WriteString(chunk)
		}
		os.Exit(0)
	}
}

func TestSecretRedaction(t *testing.T) {
	request := Request{
		Password:       "synthetic&password",
		SourcePassword: "synthetic-source-password",
		Connection: repository.NewS3Connection(repository.S3Connection{
			Endpoint: "https://redaction.example.test", Bucket: "synthetic-bucket", Prefix: "repository", Region: "test-1",
			AccessKey: "synthetic-source-access", SecretKey: "synthetic/storage+secret",
		}),
	}
	source := repository.NewSFTPConnection(repository.SFTPConnection{
		Host: "redaction.example.test", Port: 22, Username: "synthetic", Path: "/repository",
		HostKeys: []string{"ssh-ed25519 c3ludGhldGljLWtleQ=="}, Authentication: repository.PasswordAuthentication("synthetic-sftp-password"),
	})
	request.SourceConnection = &source

	got := redact("synthetic&password synthetic/storage+secret synthetic%26password synthetic-source-password synthetic-source-access synthetic-sftp-password", request)

	if got != "[REDACTED] [REDACTED] [REDACTED] [REDACTED] [REDACTED] [REDACTED]" {
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
