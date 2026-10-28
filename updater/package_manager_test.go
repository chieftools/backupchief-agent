package updater

import (
	"context"
	"slices"
	"testing"
)

type recordingRunner struct {
	commands [][]string
}

func (runner *recordingRunner) CombinedOutput(_ context.Context, name string, args ...string) ([]byte, error) {
	runner.commands = append(runner.commands, append([]string{name}, args...))
	return nil, nil
}

func TestAptInstallKeepsChangedConfigurationWithoutPrompting(t *testing.T) {
	runner := &recordingRunner{}
	manager := &aptManager{binary: "apt-get", runner: runner}

	if err := manager.Install(context.Background(), "1.2.3"); err != nil {
		t.Fatalf("install: %v", err)
	}

	want := []string{"apt-get", "install", "-y", "--allow-downgrades",
		"-o", "Dpkg::Options::=--force-confdef", "-o", "Dpkg::Options::=--force-confold", packageName + "=1.2.3"}
	if len(runner.commands) != 1 || !slices.Equal(runner.commands[0], want) {
		t.Fatalf("unexpected commands: %q", runner.commands)
	}
}
