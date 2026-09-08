package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/chieftools/backupchief-agent/agent"
)

func TestVersion(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, version, want string
		args                []string
	}{
		{"development", "", "Backup Chief agent dev\n", []string{"version"}},
		{"release", "0.4.2", "Backup Chief agent 0.4.2\n", []string{"version"}},
		{"flag", "0.4.2", "Backup Chief agent 0.4.2\n", []string{"--version"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			root := NewRootCommand(test.version)
			var output bytes.Buffer
			root.SetOut(&output)
			root.SetArgs(test.args)
			if err := root.Execute(); err != nil {
				t.Fatal(err)
			}
			if output.String() != test.want {
				t.Fatalf("got %q, want %q", output.String(), test.want)
			}
		})
	}
}

func TestHelpExplainsAgentCommands(t *testing.T) {
	t.Parallel()
	root := NewRootCommand("dev")
	var output bytes.Buffer
	root.SetOut(&output)
	root.SetArgs([]string{"--help"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"backupchief [command]", "Run backups from a local configuration", "backup", "config", "repository", "run", "setup", "version"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("help missing %q: %s", want, &output)
		}
	}
}

func TestConfigurationFlagDefaultsToTheStandaloneConfiguration(t *testing.T) {
	t.Parallel()
	root := NewRootCommand("dev")
	flag := root.PersistentFlags().Lookup("config")
	if flag == nil || flag.DefValue != agent.DefaultPaths().StandaloneConfig {
		t.Fatalf("unexpected configuration default: %+v", flag)
	}
}

func TestConfigUpdateHelpDocumentsForcedFetching(t *testing.T) {
	t.Parallel()
	root := NewRootCommand("dev")
	var output bytes.Buffer
	root.SetOut(&output)
	root.SetArgs([]string{"config", "update", "--help"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Fetch and validate", "--force", "Bypass the current configuration ETag"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("config update help missing %q: %s", want, &output)
		}
	}
}

func TestInvalidInvocationFails(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{{"unknown"}, {"version", "extra"}, {"setup"}, {"setup", "first", "second"}, {"run", "--unknown"}} {
		root := NewRootCommand("dev")
		root.SetArgs(args)
		if err := root.Execute(); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}
