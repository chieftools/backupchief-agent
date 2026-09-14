package restic

import (
	"context"
	"crypto/sha256"
	"encoding/json"
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
	repositoryLock, err := lockRepository(ctx, runner.State, request.Connection)
	if err != nil {
		result.Diagnostic = "cannot acquire repository operation lock"
		return result
	}
	defer func() {
		_ = syscall.Flock(int(repositoryLock.Fd()), syscall.LOCK_UN)
		_ = repositoryLock.Close()
	}()

	work, cleanup, err := workspace(ctx, runner.State)
	if err != nil {
		result.Diagnostic = "cannot prepare private execution workspace"
		return result
	}
	defer cleanup()

	passwordFile := filepath.Join(work, "password")
	newPasswordFile := filepath.Join(work, "new-password")
	commandConfigFile := filepath.Join(work, "command.cnf")
	cache := filepath.Join(runner.State, "cache")

	if err = privateDirectory(cache); err != nil {
		result.Diagnostic = "cannot prepare private cache"
		return result
	}

	args, env, err := request.arguments(passwordFile, newPasswordFile, cache, runner.AllowLocal)
	if err != nil {
		result.Diagnostic = err.Error()
		return result
	}

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
	if request.Operation == "backup_stdin" {
		if err := os.WriteFile(commandConfigFile, []byte(request.CommandConfig), 0600); err != nil {
			result.Diagnostic = "cannot write private command configuration"
			return result
		}
		for index, argument := range args {
			args[index] = strings.ReplaceAll(argument, "{backupchief-command-config}", commandConfigFile)
		}
	}

	if request.Connection.Driver == "s3" {
		endpoint, err := canonicalS3Endpoint(request.Connection)
		if err != nil {
			result.Diagnostic = "cannot establish guarded S3 transport"
			return result
		}

		proxy, err := egress.Start(ctx, endpoint.String())
		if err != nil {
			result.Diagnostic = "cannot establish guarded S3 transport"
			return result
		}
		defer proxy.Close()

		env = append(
			env,
			"HTTPS_PROXY="+proxy.URL,
			"HTTP_PROXY="+proxy.URL,
			"NO_PROXY=",
			"https_proxy="+proxy.URL,
			"http_proxy="+proxy.URL,
			"no_proxy=",
		)
	}

	execute := func(arguments []string, executionRequest Request) Result {
		command := exec.Command(binary, arguments...)
		command.Env = env
		command.Dir = work

		return runProcess(ctx, command, executionRequest)
	}

	if request.RecoverStaleLocks {
		unlockArgs, _, unlockErr := request.staleLockArguments(passwordFile, cache, runner.AllowLocal)
		if unlockErr != nil {
			result.Diagnostic = unlockErr.Error()
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
		request.NewPassword,
		request.Connection.AccessKey,
		request.Connection.SecretKey,
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
