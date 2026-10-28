package updater

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

// lockFile serializes helpers: the daemon starts one without systemd, also
// again after a restart while a request is pending.
const lockFile = "updater.lock"

var errUpdaterRunning = errors.New("another updater is running")

// Launch starts `backupchief internal-update` in a session of its own, for
// hosts where no systemd path unit starts it. Its output goes to logPath. The
// helper outlives the daemon, which the package's maintainer scripts restart.
func Launch(binary, logPath string) error {
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		return err
	}
	output, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("open updater log: %w", err)
	}
	defer output.Close()

	command := exec.Command(binary, "internal-update")
	command.Dir = "/"
	command.Stdout = output
	command.Stderr = output
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := command.Start(); err != nil {
		return fmt.Errorf("start updater: %w", err)
	}
	// Reap the helper when it finishes before the daemon restarts.
	go func() { _ = command.Wait() }()
	return nil
}

// lockUpdater takes the helper lock in directory, or returns errUpdaterRunning
// while another helper holds it. The lock ends with the process.
func lockUpdater(directory string) (func(), error) {
	file, err := os.OpenFile(filepath.Join(directory, lockFile), os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errUpdaterRunning
		}
		return nil, err
	}
	return func() { _ = file.Close() }, nil
}
