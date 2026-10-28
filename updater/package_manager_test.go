package updater

import (
	"context"
	"slices"
	"strings"
	"testing"
)

type recordingRunner struct {
	commands [][]string
	outputs  map[string]string
}

func (runner *recordingRunner) CombinedOutput(_ context.Context, name string, args ...string) ([]byte, error) {
	command := append([]string{name}, args...)
	runner.commands = append(runner.commands, command)
	return []byte(runner.outputs[strings.Join(command, " ")]), nil
}

func TestAptInstallKeepsChangedConfigurationWithoutPrompting(t *testing.T) {
	runner := &recordingRunner{}
	manager := &aptManager{binary: "apt-get", runner: runner}

	if err := manager.Install(context.Background(), "1.2.3"); err != nil {
		t.Fatalf("install: %v", err)
	}

	want := []string{"apt-get", "install", "-y", "--allow-downgrades",
		"-o", "Dpkg::Options::=--force-confdef", "-o", "Dpkg::Options::=--force-confold", packageName + "=1.2.3"}
	if len(runner.commands) != 2 || !slices.Equal(runner.commands[1], want) {
		t.Fatalf("unexpected commands: %q", runner.commands)
	}
}

func TestAptInstallForcesDowngradesOnAptBefore11(t *testing.T) {
	runner := &recordingRunner{outputs: map[string]string{"apt-get --version": "apt 1.0.1ubuntu2.24 for amd64 compiled on Oct 10 2019 22:48:21\n"}}
	manager := &aptManager{binary: "apt-get", runner: runner}

	if err := manager.Install(context.Background(), "1.2.3"); err != nil {
		t.Fatalf("install: %v", err)
	}

	want := []string{"apt-get", "install", "-y", "--force-yes",
		"-o", "Dpkg::Options::=--force-confdef", "-o", "Dpkg::Options::=--force-confold", packageName + "=1.2.3"}
	if len(runner.commands) != 2 || !slices.Equal(runner.commands[1], want) {
		t.Fatalf("unexpected commands: %q", runner.commands)
	}
}

func TestAptVersionBefore11(t *testing.T) {
	for output, want := range map[string]bool{
		"apt 1.0.1ubuntu2.24 for amd64 compiled on Oct 10 2019 22:48:21": true,
		"apt 1.0.9.8.6 for amd64 compiled on May 14 2020 16:52:22":       true,
		"apt 1.1.10 for amd64 compiled on Jan 1 2016":                    false,
		"apt 2.8.3 (amd64)": false,
		"":                  false,
	} {
		if got := aptVersionBefore11(output); got != want {
			t.Errorf("aptVersionBefore11(%q) = %v, want %v", output, got, want)
		}
	}
}
