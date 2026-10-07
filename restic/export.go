package restic

import (
	"archive/zip"
	"compress/flate"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/chieftools/backupchief-agent/repository"
)

type ExportRequest struct {
	Version          int        `json:"version"`
	Connection       Connection `json:"connection"`
	Password         string     `json:"password"`
	Snapshot         string     `json:"snapshot"`
	Kind             string     `json:"kind"`
	Path             string     `json:"path"`
	ArchiveEntryName string     `json:"archive_entry_name"`
	TimeoutSeconds   int        `json:"timeout_seconds"`
	LockWaitSeconds  int        `json:"lock_wait_seconds"`
	// Scratch is an optional local directory where the control plane lets the selection be
	// restored before zipping, which reads the repository pack by pack instead of blob by blob.
	Scratch string `json:"scratch,omitempty"`
	// CompressionWorkers is how many cores may deflate one large entry; zero or one compresses
	// sequentially. The control plane sets it from how many exports may run side by side.
	CompressionWorkers int `json:"compression_workers,omitempty"`
}

const maxCompressionWorkers = 16

type ExportInspection struct {
	Version     int   `json:"version"`
	SourceBytes int64 `json:"source_bytes"`
}

func (runner Runner) InspectExport(ctx context.Context, request ExportRequest) (ExportInspection, error) {
	command, cleanup, err := runner.exportCommand(ctx, request, "inspect", "")
	if err != nil {
		return ExportInspection{}, err
	}
	defer cleanup()

	stdout, err := command.StdoutPipe()
	if err != nil {
		return ExportInspection{}, errors.New("cannot read verified restic output")
	}
	stderr := &boundedOutput{limit: 256 << 10}
	command.Stderr = stderr

	if err = command.Start(); err != nil {
		return ExportInspection{}, errors.New("cannot start verified restic executable")
	}

	decoder := json.NewDecoder(stdout)
	var total int64
	var selected bool

	for {
		var node struct {
			StructType string `json:"struct_type"`
			Path       string `json:"path"`
			Type       string `json:"type"`
			Size       int64  `json:"size"`
		}

		if err = decoder.Decode(&node); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			stopExportCommand(command)
			return ExportInspection{}, errors.New("invalid restic inspection output")
		}

		if node.StructType != "node" {
			continue
		}
		if node.Path == request.Path {
			selected = true
		}
		if node.Type == "file" {
			if node.Size < 0 || total > math.MaxInt64-node.Size {
				stopExportCommand(command)
				return ExportInspection{}, errors.New("snapshot selection size overflow")
			}
			total += node.Size
		}
	}

	if err = command.Wait(); err != nil || !selected {
		return ExportInspection{}, errors.New("snapshot selection is unavailable")
	}

	return ExportInspection{Version: protocolVersion, SourceBytes: total}, nil
}

// ExportStats describes how an archive was produced, for diagnosing slow exports.
type ExportStats struct {
	// Scratch is whether the archive was zipped from a scratch restore instead of streamed.
	Scratch bool
	// Restore is the time spent restoring to scratch space, including a restore that failed.
	Restore time.Duration
}

func (runner Runner) StreamExport(ctx context.Context, request ExportRequest, output io.Writer) (ExportStats, error) {
	var stats ExportStats

	if request.Scratch != "" {
		started := time.Now()
		restored, cleanup, err := runner.restoreExport(ctx, request)
		stats.Restore = time.Since(started)
		if err == nil {
			defer cleanup()
			stats.Scratch = true

			return stats, writeRestoredArchive(restored, request, output)
		}
		// Nothing was written yet, so a failed restore quietly falls back to streaming the dump.
	}

	command, cleanup, err := runner.exportCommand(ctx, request, "archive", "")
	if err != nil {
		return stats, err
	}
	defer cleanup()

	command.Stderr = &boundedOutput{limit: 256 << 10}

	if request.Kind == "directory" {
		command.Stdout = output
		if err = command.Run(); err != nil {
			return stats, errors.New("snapshot archive failed")
		}

		return stats, nil
	}

	archive := newArchiveWriter(output, request.CompressionWorkers)
	entry, err := archive.Create(request.ArchiveEntryName)
	if err != nil {
		return stats, errors.New("cannot create snapshot archive entry")
	}
	command.Stdout = entry

	if err = command.Run(); err != nil {
		_ = archive.Close()
		return stats, errors.New("snapshot archive failed")
	}
	if err = archive.Close(); err != nil {
		return stats, errors.New("cannot finalize snapshot archive")
	}

	return stats, nil
}

// newArchiveWriter deflates entries at the fastest level, on several cores when workers allows
// it: single-core compression is what bounds the export of a large, compressible selection such
// as a database dump.
func newArchiveWriter(output io.Writer, workers int) *zip.Writer {
	archive := zip.NewWriter(output)
	archive.RegisterCompressor(zip.Deflate, func(writer io.Writer) (io.WriteCloser, error) {
		if workers > 1 {
			return newParallelDeflater(writer, workers), nil
		}

		return flate.NewWriter(writer, flate.BestSpeed)
	})

	return archive
}

func (runner Runner) exportCommand(ctx context.Context, request ExportRequest, mode string, target string) (*exec.Cmd, func(), error) {
	if mode != "inspect" && mode != "archive" && mode != "restore" {
		return nil, nil, errors.New("invalid snapshot export mode")
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(request.TimeoutSeconds)*time.Second)
	fail := func(err error) (*exec.Cmd, func(), error) {
		cancel()
		return nil, nil, err
	}

	baseRequest := Request{
		Version:         request.Version,
		Connection:      request.Connection,
		Password:        request.Password,
		TimeoutSeconds:  request.TimeoutSeconds,
		LockWaitSeconds: request.LockWaitSeconds,
	}
	if !validExportSelection(request) || !snapshotPattern.MatchString(request.Snapshot) {
		return fail(errors.New("invalid snapshot export request"))
	}
	if err := privateDirectory(runner.State); err != nil {
		return fail(errors.New("invalid private state directory"))
	}

	repositoryLock, err := lockRepositoryShared(ctx, runner.State, request.Connection)
	if err != nil {
		return fail(errors.New("cannot acquire repository operation lock"))
	}
	unlock := func() {
		_ = syscall.Flock(int(repositoryLock.Fd()), syscall.LOCK_UN)
		_ = repositoryLock.Close()
	}

	work, cleanWork, err := workspace(ctx, runner.State)
	if err != nil {
		unlock()
		return fail(errors.New("cannot prepare private execution workspace"))
	}
	cleanup := func() {
		cleanWork()
		unlock()
		cancel()
	}

	cache := filepath.Join(runner.State, "cache")
	if err = privateDirectory(cache); err != nil {
		cleanup()
		return nil, nil, errors.New("cannot prepare private cache")
	}
	passwordFile := filepath.Join(work, "password")
	if err = os.WriteFile(passwordFile, []byte(request.Password), 0600); err != nil {
		cleanup()
		return nil, nil, errors.New("cannot write private password input")
	}

	session, err := repository.OpenSession(ctx, repository.SessionOptions{
		State: runner.State, Work: work, AllowLocal: runner.AllowLocal,
	}, []repository.Binding{{Name: "backupchief_repository", Connection: request.Connection, Strategy: repository.PreferNative}})
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	previousCleanup := cleanup
	cleanup = func() {
		session.Close()
		previousCleanup()
	}
	prepared, _ := session.Repository("backupchief_repository")
	args, env, err := baseRequest.baseArgumentsPrepared(passwordFile, cache, prepared)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	env = append(env, session.Environment()...)

	if mode == "inspect" {
		args = append(args, "ls", "--recursive", "--json", request.Snapshot, request.Path)
	} else if mode == "restore" {
		args = append(args, restoreArguments(request, target)...)
		// Restores run on the control plane's own hardware, so they may read in parallel.
		env = append(env, "GOMAXPROCS="+strconv.Itoa(runtime.NumCPU()), "RESTIC_READ_CONCURRENCY="+strconv.Itoa(scratchReadConcurrency))
	} else if request.Path == "/" {
		args = append(args, "dump", "--archive", "zip", request.Snapshot+":/", "/")
	} else if request.Kind == "directory" {
		parent := filepath.Dir(request.Path)
		args = append(args, "dump", "--archive", "zip", request.Snapshot+":"+parent, "/"+filepath.Base(request.Path))
	} else {
		parent := filepath.Dir(request.Path)
		args = append(args, "dump", request.Snapshot+":"+parent, "/"+filepath.Base(request.Path))
	}

	binary, err := Binary(ctx, runner.State)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	command := exec.CommandContext(ctx, binary, args...)
	command.Env = env
	command.Dir = work
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		if command.Process == nil {
			return os.ErrProcessDone
		}
		if err := syscall.Kill(-command.Process.Pid, syscall.SIGTERM); err != nil {
			if errors.Is(err, syscall.ESRCH) {
				return os.ErrProcessDone
			}
			return err
		}

		return nil
	}
	command.WaitDelay = 10 * time.Second

	return command, cleanup, nil
}

func stopExportCommand(command *exec.Cmd) {
	if command.Process == nil {
		return
	}

	_ = syscall.Kill(-command.Process.Pid, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		_ = command.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		<-done
	}
}

func validExportSelection(request ExportRequest) bool {
	if request.CompressionWorkers < 0 || request.CompressionWorkers > maxCompressionWorkers {
		return false
	}
	if request.Kind != "file" && request.Kind != "directory" && request.Kind != "database" {
		return false
	}
	if !safeSnapshotPath(request.Path) {
		return false
	}
	if request.Path == "/" {
		return request.Kind == "directory" && request.ArchiveEntryName == "backup"
	}
	if request.ArchiveEntryName != filepath.Base(request.Path) || strings.ContainsAny(request.ArchiveEntryName, "/\\\x00\r\n") {
		return false
	}
	return len(request.ArchiveEntryName) > 0 && len(request.ArchiveEntryName) <= 4096
}
