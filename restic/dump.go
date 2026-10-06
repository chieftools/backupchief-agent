package restic

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

const maxDumpBytes = 64 << 20

// dumpOutput writes one file's bytes to a private target and stops Restic once the cap is crossed,
// so the control plane can hand a single small file to a browser without buffering it in JSON.
type dumpOutput struct {
	mu       sync.Mutex
	file     *os.File
	limit    int64
	written  int64
	overflow bool
	failed   bool
	closed   bool
	cancel   context.CancelFunc
}

func newDumpOutput(target string, limit int64, cancel context.CancelFunc) (*dumpOutput, error) {
	file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}

	return &dumpOutput{file: file, limit: limit, cancel: cancel}, nil
}

func (output *dumpOutput) Write(data []byte) (int, error) {
	output.mu.Lock()
	defer output.mu.Unlock()

	if output.overflow || output.failed {
		return len(data), nil
	}

	if output.written+int64(len(data)) > output.limit {
		output.overflow = true
		output.cancel()

		return len(data), nil
	}

	if _, err := output.file.Write(data); err != nil {
		output.failed = true
		output.cancel()

		return len(data), nil
	}

	output.written += int64(len(data))

	return len(data), nil
}

// Close keeps the file only when the dump completed within its cap. Only the first call decides.
func (output *dumpOutput) Close(keep bool) {
	output.mu.Lock()
	defer output.mu.Unlock()

	if output.closed {
		return
	}
	output.closed = true

	_ = output.file.Close()
	if !keep || output.overflow || output.failed {
		_ = os.Remove(output.file.Name())
	}
}

func (output *dumpOutput) state() (written int64, overflow bool, failed bool) {
	output.mu.Lock()
	defer output.mu.Unlock()

	return output.written, output.overflow, output.failed
}

func safeDumpTarget(target string) bool {
	if !filepath.IsAbs(target) || filepath.Clean(target) != target || filepath.Base(target) == "." {
		return false
	}

	info, err := os.Lstat(filepath.Dir(target))

	return err == nil && info.IsDir()
}
