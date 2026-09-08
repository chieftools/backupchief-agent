package restic

import (
	"bytes"
	"compress/bzip2"
	"context"
	"crypto/sha256"
	"debug/elf"
	"debug/macho"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
)

//go:embed VERSION
var versionFile string

//go:embed BINARY_SHA256SUMS
var binaryChecksums string

var Version = strings.TrimSpace(versionFile)

func checksumFor(platform string) string {
	for _, line := range strings.Split(binaryChecksums, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[1] == platform {
			return fields[0]
		}
	}
	panic("missing Restic checksum for " + platform)
}

func Binary(ctx context.Context, state string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	if runtime.GOOS != "linux" && (runtime.GOOS != "darwin" || runtime.GOARCH != "arm64") {
		return "", errors.New("this platform does not have an embedded restic executable")
	}

	if err := privateDirectory(state); err != nil {
		return "", err
	}

	lock, err := os.OpenFile(filepath.Join(state, ".restic-extract.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return "", err
	}
	defer lock.Close()

	if err = acquire(ctx, lock); err != nil {
		return "", err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)

	target := filepath.Join(state, "restic-"+Version+"-"+runtime.GOARCH)
	if _, err := os.Lstat(target); err == nil {
		return target, verifyFile(target, binaryHash)
	} else if !os.IsNotExist(err) {
		return "", err
	}

	stale, err := filepath.Glob(filepath.Join(state, ".restic-extract-*.tmp"))
	if err != nil {
		return "", err
	}

	for _, path := range stale {
		if err := os.Remove(path); err != nil {
			return "", err
		}
	}

	temp, err := os.CreateTemp(state, ".restic-extract-*.tmp")
	if err != nil {
		return "", err
	}
	defer os.Remove(temp.Name())
	defer temp.Close()

	source := contextReader{
		ctx:    ctx,
		reader: bzip2.NewReader(bytes.NewReader(embedded)),
	}
	if _, err = io.Copy(temp, io.LimitReader(source, 128<<20)); err != nil {
		return "", err
	}
	if err = temp.Chmod(0500); err != nil {
		return "", err
	}
	if err = temp.Sync(); err != nil {
		return "", err
	}
	if err = temp.Close(); err != nil {
		return "", err
	}
	if err = verifyFile(temp.Name(), binaryHash); err != nil {
		return "", err
	}
	if err = os.Rename(temp.Name(), target); err != nil {
		return "", err
	}

	dir, err := os.Open(state)
	if err != nil {
		return "", err
	}
	defer dir.Close()
	if err = dir.Sync(); err != nil {
		return "", err
	}

	return target, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader contextReader) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}

	return reader.reader.Read(buffer)
}

func verifyFile(path, expected string) error {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return errors.New("restic executable cannot be opened safely")
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 || info.Mode().Perm()&0100 == 0 {
		return errors.New("unsafe restic executable permissions")
	}

	hash := sha256.New()
	if _, err := io.Copy(hash, io.LimitReader(file, 128<<20)); err != nil {
		return err
	}

	if fmt.Sprintf("%x", hash.Sum(nil)) != expected {
		return errors.New("restic executable checksum mismatch")
	}

	if runtime.GOOS == "linux" {
		executable, err := elf.NewFile(file)
		if err != nil {
			return errors.New("restic executable is not ELF")
		}

		expectedMachine := elf.EM_X86_64
		if runtime.GOARCH == "arm64" {
			expectedMachine = elf.EM_AARCH64
		}
		if executable.Machine != expectedMachine {
			return errors.New("restic executable architecture mismatch")
		}
	} else if runtime.GOOS == "darwin" {
		executable, err := macho.NewFile(file)
		if err != nil || executable.Cpu != macho.CpuArm64 {
			return errors.New("restic executable architecture mismatch")
		}
	}

	return nil
}

func privateDirectory(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("state directory must be absolute")
	}

	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}

	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("state directory must be a private directory")
	}

	return nil
}

func acquire(ctx context.Context, file *os.File) error {
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
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
