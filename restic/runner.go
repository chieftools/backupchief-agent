package restic

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/chieftools/backupchief-agent/egress"
	bundledrclone "github.com/chieftools/backupchief-agent/rclone"
)

type Runner struct {
	State      string
	AllowLocal bool
}

func (runner Runner) Run(ctx context.Context, request Request) Result {
	result := Result{Version: protocolVersion, ExitCode: 1, Outcome: "failed"}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(request.TimeoutSeconds)*time.Second)
	defer cancel()

	if err := privateDirectory(runner.State); err != nil {
		result.Diagnostic = "invalid private state directory"
		return result
	}
	shared := request.Operation == "backup" || request.Operation == "backup_stdin"
	var repositoryLock *os.File
	var err error
	if shared {
		repositoryLock, err = lockRepositoryShared(ctx, runner.State, request.Connection)
	} else {
		repositoryLock, err = lockRepository(ctx, runner.State, request.Connection)
	}
	if err != nil {
		result.Diagnostic = "cannot acquire repository operation lock"
		return result
	}
	defer func() {
		_ = syscall.Flock(int(repositoryLock.Fd()), syscall.LOCK_UN)
		_ = repositoryLock.Close()
	}()
	var sourceLock *os.File
	if request.SourceConnection != nil {
		sourceLock, err = lockRepositoryShared(ctx, runner.State, *request.SourceConnection)
		if err != nil {
			result.Diagnostic = "cannot acquire source repository operation lock"
			return result
		}
		defer func() {
			_ = syscall.Flock(int(sourceLock.Fd()), syscall.LOCK_UN)
			_ = sourceLock.Close()
		}()
	}

	work, cleanup, err := workspace(ctx, runner.State)
	if err != nil {
		result.Diagnostic = "cannot prepare private execution workspace"
		return result
	}
	defer cleanup()

	passwordFile := filepath.Join(work, "password")
	sourcePasswordFile := filepath.Join(work, "source-password")
	newPasswordFile := filepath.Join(work, "new-password")
	rcloneConfigFile := filepath.Join(work, "rclone.conf")
	commandConfigFile := filepath.Join(work, "command.cnf")
	cache := filepath.Join(runner.State, "cache")

	if err = privateDirectory(cache); err != nil {
		result.Diagnostic = "cannot prepare private cache"
		return result
	}

	connections := []Connection{request.Connection}
	if request.SourceConnection != nil {
		connections = append(connections, *request.SourceConnection)
	}
	dual := request.Operation == "init_from" || request.Operation == "copy"
	transport, proxyEnvironment, closeTransport, err := prepareTransport(ctx, runner.State, work, connections, dual)
	if err != nil {
		result.Diagnostic = err.Error()
		return result
	}
	defer closeTransport()

	var args []string
	var env []string
	var rcloneConfig string
	if request.Operation == "init_from" || request.Operation == "copy" {
		transport.rcloneConfig = rcloneConfigFile
		args, env, rcloneConfig, err = request.dualArguments(passwordFile, sourcePasswordFile, cache, transport, runner.AllowLocal)
	} else {
		transport.rcloneConfig = rcloneConfigFile
		args, env, rcloneConfig, err = request.argumentsWithTransport(passwordFile, newPasswordFile, cache, transport, runner.AllowLocal)
	}
	if err != nil {
		result.Diagnostic = err.Error()
		return result
	}
	env = append(env, proxyEnvironment...)

	binary, err := Binary(ctx, runner.State)
	if err != nil {
		result.Diagnostic = err.Error()
		return result
	}

	if request.Operation == "backup" {
		for i, arg := range args {
			if arg == "--" {
				args = slices.Insert(args, i, "--exclude", runner.State)
				break
			}
		}
	}

	if err := os.WriteFile(passwordFile, []byte(request.Password), 0600); err != nil {
		result.Diagnostic = "cannot write private password input"
		return result
	}
	if request.NewPassword != "" {
		if err := os.WriteFile(newPasswordFile, []byte(request.NewPassword), 0600); err != nil {
			result.Diagnostic = "cannot write private password input"
			return result
		}
	}
	if request.SourcePassword != "" {
		if err := os.WriteFile(sourcePasswordFile, []byte(request.SourcePassword), 0600); err != nil {
			result.Diagnostic = "cannot write private source password input"
			return result
		}
	}
	if rcloneConfig != "" {
		if err := os.WriteFile(rcloneConfigFile, []byte(rcloneConfig), 0600); err != nil {
			result.Diagnostic = "cannot write private transport configuration"
			return result
		}
	}
	if request.Operation == "backup_stdin" {
		if err := os.WriteFile(commandConfigFile, []byte(request.CommandConfig), 0600); err != nil {
			result.Diagnostic = "cannot write private command configuration"
			return result
		}
		for index, argument := range args {
			args[index] = strings.ReplaceAll(argument, "{backupchief-command-config}", commandConfigFile)
		}
	}

	execute := func(arguments []string, executionRequest Request) Result {
		command := exec.Command(binary, arguments...)
		command.Env = env
		command.Dir = work

		return runProcess(ctx, command, executionRequest)
	}

	if request.RecoverStaleLocks {
		unlockArgs, _, unlockConfig, unlockErr := request.staleLockArgumentsWithTransport(passwordFile, cache, transport, runner.AllowLocal)
		if unlockErr != nil {
			result.Diagnostic = unlockErr.Error()
			return result
		}
		if unlockConfig != "" && unlockConfig != rcloneConfig {
			result.Diagnostic = "cannot prepare private transport configuration"
			return result
		}
		unlocked := execute(unlockArgs, request)
		if ctx.Err() != nil {
			return unlocked
		}

		result = execute(args, request)
		result.Diagnostic = strings.TrimSpace(joinResultLogs("stale repository lock cleanup attempted before maintenance", unlocked) + "\n" + result.Diagnostic)
		return result
	}

	return execute(args, request)
}

func prepareTransport(ctx context.Context, state, work string, connections []Connection, dual bool) (transportOptions, []string, func(), error) {
	options := transportOptions{}
	environment := []string{}
	cleanup := func() {}
	targets := make([]egress.Target, 0, len(connections))
	requiresRclone := false

	for _, connection := range connections {
		switch connection.Driver {
		case "s3":
			endpoint, err := canonicalS3Endpoint(connection)
			if err != nil {
				return transportOptions{}, nil, cleanup, errors.New("cannot establish guarded storage transport")
			}
			targets = append(targets, egress.Target{Host: endpoint.Hostname(), Port: 443})
			if dual {
				requiresRclone = true
			}
		case "sftp":
			targets = append(targets, egress.Target{Host: connection.Host, Port: connection.Port})
			requiresRclone = true
		case "local":
		default:
			return transportOptions{}, nil, cleanup, errors.New("unsupported repository driver")
		}
	}
	if dual {
		for _, connection := range connections {
			if connection.Driver != "local" {
				requiresRclone = true
			}
		}
	}

	if len(targets) > 0 {
		proxy, err := egress.StartTargets(ctx, targets)
		if err != nil {
			return transportOptions{}, nil, cleanup, errors.New("cannot establish guarded storage transport")
		}
		cleanup = proxy.Close
		options.proxyURL = proxy.URL
		environment = append(environment,
			"HTTPS_PROXY="+proxy.URL,
			"HTTP_PROXY="+proxy.URL,
			"NO_PROXY=",
			"https_proxy="+proxy.URL,
			"http_proxy="+proxy.URL,
			"no_proxy=",
		)
	}

	if requiresRclone {
		program, err := bundledrclone.Binary(ctx, state)
		if err != nil {
			cleanup()
			return transportOptions{}, nil, func() {}, errors.New("cannot prepare bundled rclone executable")
		}
		options.rcloneProgram = program
		obscuredPasswords := map[string]string{}
		options.obscure = func(password string) (string, error) {
			if obscured, exists := obscuredPasswords[password]; exists {
				return obscured, nil
			}
			command := exec.CommandContext(ctx, program, "obscure", "-")
			command.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + work, "TMPDIR=" + work, "LANG=C"}
			command.Dir = work
			command.Stdin = strings.NewReader(password)
			output, err := command.Output()
			if err != nil {
				return "", err
			}
			obscured := strings.TrimSpace(string(output))
			obscuredPasswords[password] = obscured
			return obscured, nil
		}
	}

	return options, environment, cleanup, nil
}

func joinResultLogs(prefix string, results ...Result) string {
	parts := []string{prefix}
	for _, result := range results {
		parts = append(parts, result.Output, result.Diagnostic)
	}

	joined := make([]string, 0, len(parts))
	for _, part := range parts {
		if strings.TrimSpace(part) != "" {
			joined = append(joined, strings.TrimSpace(part))
		}
	}

	return strings.Join(joined, "\n")
}

func lockRepository(ctx context.Context, state string, connection Connection) (*os.File, error) {
	return lockRepositoryMode(ctx, state, connection, false)
}

func lockRepositoryShared(ctx context.Context, state string, connection Connection) (*os.File, error) {
	return lockRepositoryMode(ctx, state, connection, true)
}

func lockRepositoryMode(ctx context.Context, state string, connection Connection, shared bool) (*os.File, error) {
	locks := filepath.Join(state, "repository-locks")
	if err := privateDirectory(locks); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(connection)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(encoded)
	path := filepath.Join(locks, fmt.Sprintf("%x.lock", digest))
	lock, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	if err := acquireRepository(ctx, lock, shared); err != nil {
		_ = lock.Close()
		return nil, err
	}
	return lock, nil
}

func acquireRepository(ctx context.Context, file *os.File, shared bool) error {
	mode := syscall.LOCK_EX
	if shared {
		mode = syscall.LOCK_SH
	}

	for {
		err := syscall.Flock(int(file.Fd()), mode|syscall.LOCK_NB)
		if err == nil {
			return nil
		}
		if err != syscall.EWOULDBLOCK {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func runProcess(ctx context.Context, command *exec.Cmd, request Request) Result {
	result := Result{Version: protocolVersion, ExitCode: 1, Outcome: "failed"}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	outputLimit := 8 << 20
	if request.Operation == "ls" {
		outputLimit = 4 << 20
	}
	stdout := &boundedOutput{limit: outputLimit, cancel: cancel}
	stderr := &boundedOutput{limit: 256 << 10, cancel: cancel}
	if request.Operation == "backup" || request.Operation == "backup_stdin" {
		stdout.cancel = nil
		stdout.tailLimit = 1 << 20
		stderr.cancel = nil
		stderr.tailLimit = 64 << 10
	}
	command.Stdout = stdout
	command.Stderr = stderr
	command.WaitDelay = 10 * time.Second

	if err := command.Start(); err != nil {
		result.Diagnostic = "cannot start verified restic executable"
		return result
	}

	done := make(chan error, 1)

	go func() {
		done <- command.Wait()
	}()

	var err error

	select {
	case err = <-done:
	case <-ctx.Done():
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGTERM)
		select {
		case err = <-done:
		case <-time.After(10 * time.Second):
			_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
			err = <-done
		}
	}

	// The operation owns the entire process group, including a child left behind by its leader.
	_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)

	result.ExitCode = command.ProcessState.ExitCode()
	if result.ExitCode < 0 {
		result.ExitCode = 1
	}

	result.Truncated = stdout.overflow || stderr.overflow
	result.DroppedBytes = stdout.dropped + stderr.dropped
	if result.Truncated && request.Operation != "backup" {
		result.Diagnostic = "restic output exceeded its limit"
		return result
	}

	result.Output = redact(stdout.String(), request)
	result.Diagnostic = redact(stderr.String(), request)

	if ctx.Err() != nil {
		result.Outcome = "cancelled"
		result.Diagnostic = "restic execution cancelled or timed out"
		return result
	}
	if err == nil {
		result.Outcome = "complete"
	} else if result.ExitCode == 3 && (request.Operation == "backup" || request.Operation == "backup_stdin") {
		result.Outcome = "partial"
	} else if result.ExitCode == 11 {
		result.Outcome = "locked"
	}

	return result
}

type boundedOutput struct {
	mu        sync.Mutex
	data      []byte
	limit     int
	overflow  bool
	cancel    context.CancelFunc
	tailLimit int
	tail      []byte
	dropped   int
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.tailLimit > 0 {
		prefixLimit := b.limit - b.tailLimit
		remaining := p
		if len(b.data) < prefixLimit {
			count := min(len(remaining), prefixLimit-len(b.data))
			b.data = append(b.data, remaining[:count]...)
			remaining = remaining[count:]
		}
		if len(remaining) > 0 {
			b.tail = append(b.tail, remaining...)
			if len(b.tail) > b.tailLimit {
				discard := len(b.tail) - b.tailLimit
				b.tail = append([]byte(nil), b.tail[discard:]...)
				b.dropped += discard
				b.overflow = true
			}
		}
		return len(p), nil
	}

	if len(b.data)+len(p) > b.limit {
		b.overflow = true
		b.dropped += len(p)
		if b.cancel != nil {
			b.cancel()
		}
		return len(p), nil
	}

	if !b.overflow {
		b.data = append(b.data, p...)
	}

	return len(p), nil
}

func (b *boundedOutput) String() string {
	return string(b.data) + string(b.tail)
}

func redact(text string, request Request) string {
	secrets := []string{
		request.Password,
		request.SourcePassword,
		request.NewPassword,
		request.Connection.AccessKey,
		request.Connection.SecretKey,
		request.Connection.SFTPPassword,
		request.Connection.SFTPPrivateKey,
	}
	if request.SourceConnection != nil {
		secrets = append(secrets, request.SourceConnection.AccessKey, request.SourceConnection.SecretKey, request.SourceConnection.SFTPPassword, request.SourceConnection.SFTPPrivateKey)
	}

	for _, secret := range secrets {
		if secret != "" {
			for _, value := range []string{secret, url.QueryEscape(secret), url.PathEscape(secret)} {
				text = strings.ReplaceAll(text, value, "[REDACTED]")
			}
		}
	}

	return text
}

func workspace(ctx context.Context, state string) (string, func(), error) {
	root := filepath.Join(state, "work")
	if err := privateDirectory(root); err != nil {
		return "", nil, err
	}

	// Serialize creation and stale cleanup so a new directory is never mistaken for abandoned work.
	guard, err := os.OpenFile(filepath.Join(state, ".work.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return "", nil, err
	}
	defer guard.Close()
	if err = acquire(ctx, guard); err != nil {
		return "", nil, err
	}
	defer syscall.Flock(int(guard.Fd()), syscall.LOCK_UN)

	entries, err := os.ReadDir(root)
	if err != nil {
		return "", nil, err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "run-") {
			continue
		}

		path := filepath.Join(root, entry.Name())
		lock, err := os.OpenFile(filepath.Join(path, ".lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
		if err != nil {
			return "", nil, err
		}
		if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil {
			err = os.RemoveAll(path)
		}
		_ = lock.Close()

		if err != nil {
			return "", nil, err
		}
	}

	path, err := os.MkdirTemp(root, "run-")
	if err != nil {
		return "", nil, err
	}
	lock, err := os.OpenFile(filepath.Join(path, ".lock"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		_ = os.RemoveAll(path)
		return "", nil, err
	}

	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		_ = lock.Close()
		_ = os.RemoveAll(path)
		return "", nil, err
	}

	cleanup := func() {
		_ = os.RemoveAll(path)
		_ = lock.Close()
	}

	return path, cleanup, nil
}

var _ io.Writer = (*boundedOutput)(nil)
