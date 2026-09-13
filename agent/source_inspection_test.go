package agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSourceInspectionUsesAgentStateForPrivateMySQLCredentials(t *testing.T) {
	installInspectionTools(t, successfulMySQLTool, successfulMySQLDumpTool)
	stateDirectory := t.TempDir()
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "missing-global-temp"))

	result := inspectSource(context.Background(), 1, "01k4p4k2n8d3r6t9v1w5x7yabc", stateDirectory, inspectionMySQLPayload())

	if result.Status != "complete" || result.ResultCode != "success" || result.Failure != nil {
		t.Fatalf("result: %+v", result)
	}
	if len(result.Databases) != 1 || result.Databases[0] != "synthetic_app" {
		t.Fatalf("databases: %v", result.Databases)
	}
	entries, err := os.ReadDir(stateDirectory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("credential workspace was not cleaned up: %v %v", entries, err)
	}
}

func TestSourceInspectionReportsCredentialWorkspaceFailure(t *testing.T) {
	installInspectionTools(t, successfulMySQLTool, successfulMySQLDumpTool)
	payload := inspectionMySQLPayload()
	payload.Source["password"] = "synthetic-secret"

	result := inspectSource(context.Background(), 1, "01k4p4k2n8d3r6t9v1w5x7yabc", filepath.Join(t.TempDir(), "missing-state"), payload)

	assertInspectionFailure(t, result, "execution_failed", "credential_setup")
	if strings.Contains(result.Failure.Detail, "synthetic-secret") || !strings.Contains(result.Failure.Detail, "missing-state") {
		t.Fatalf("unsafe or unhelpful detail: %q", result.Failure.Detail)
	}
}

func TestSourceInspectionReportsRedactedDatabaseDiscoveryFailure(t *testing.T) {
	installInspectionTools(t, failingMySQLTool, successfulMySQLDumpTool)
	payload := inspectionMySQLPayload()
	payload.Source["password"] = "synthetic-secret"

	result := inspectSource(context.Background(), 1, "01k4p4k2n8d3r6t9v1w5x7yabc", t.TempDir(), payload)

	assertInspectionFailure(t, result, "source_authentication_failed", "database_discovery")
	if strings.Contains(result.Failure.Detail, "synthetic-secret") || strings.ContainsAny(result.Failure.Detail, "\r\n") || !strings.Contains(result.Failure.Detail, "[redacted]") {
		t.Fatalf("detail was not normalized and redacted: %q", result.Failure.Detail)
	}
}

func TestSourceInspectionReportsRedactedDumpFailure(t *testing.T) {
	installInspectionTools(t, successfulMySQLTool, failingMySQLDumpTool)
	payload := inspectionMySQLPayload()
	payload.Source["password"] = "synthetic-secret"

	result := inspectSource(context.Background(), 1, "01k4p4k2n8d3r6t9v1w5x7yabc", t.TempDir(), payload)

	assertInspectionFailure(t, result, "source_authentication_failed", "dump_test")
	if strings.Contains(result.Failure.Detail, "synthetic-secret") || !strings.Contains(result.Failure.Detail, "[redacted]") {
		t.Fatalf("detail was not redacted: %q", result.Failure.Detail)
	}
}

func TestSourceInspectionClassifiesValidationAccessAndToolFailures(t *testing.T) {
	t.Run("source validation", func(t *testing.T) {
		result := inspectSource(context.Background(), 1, "01k4p4k2n8d3r6t9v1w5x7yabc", t.TempDir(), CommandPayload{Type: "mysql", Source: map[string]any{"host": "database.example.test"}})

		assertInspectionFailure(t, result, "execution_failed", "source_validation")
	})

	t.Run("source access", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "missing-source")
		result := inspectSource(context.Background(), 1, "01k4p4k2n8d3r6t9v1w5x7yabc", t.TempDir(), CommandPayload{Type: "file", Source: map[string]any{"root": root}})

		assertInspectionFailure(t, result, "invalid_root", "source_access")
		if !strings.Contains(result.Failure.Detail, root) {
			t.Fatalf("detail does not identify the source root: %q", result.Failure.Detail)
		}
	})

	t.Run("tool check", func(t *testing.T) {
		t.Setenv("PATH", t.TempDir())
		result := inspectSource(context.Background(), 1, "01k4p4k2n8d3r6t9v1w5x7yabc", t.TempDir(), inspectionMySQLPayload())

		assertInspectionFailure(t, result, "tool_unavailable", "tool_check")
	})
}

func TestInspectionFailureLimitsUnicodeDetail(t *testing.T) {
	failure := inspectionFailure("unknown", "synthetic-secret\n"+strings.Repeat("ø", 1100), "synthetic-secret")

	if strings.Contains(failure.Detail, "synthetic-secret") || strings.ContainsAny(failure.Detail, "\r\n") || utf8.RuneCountInString(failure.Detail) != 1000 {
		t.Fatalf("detail: %q", failure.Detail)
	}
}

func inspectionMySQLPayload() CommandPayload {
	return CommandPayload{
		Type: "mysql",
		Source: map[string]any{
			"host":     "database.example.test",
			"port":     3306,
			"username": "synthetic_reader",
			"password": "",
			"selection": map[string]any{
				"mode":      "all_accessible",
				"databases": []string{},
			},
			"dump": map[string]any{
				"include_routines": false,
				"include_events":   false,
				"custom_flags":     []string{},
			},
		},
	}
}

func installInspectionTools(t *testing.T, mysql, mysqldump string) {
	t.Helper()
	directory := t.TempDir()
	writeInspectionTool(t, directory, "mysql", mysql)
	writeInspectionTool(t, directory, "mysqldump", mysqldump)
	t.Setenv("PATH", directory)
}

func writeInspectionTool(t *testing.T, directory, name, contents string) {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte(contents), 0o700); err != nil {
		t.Fatal(err)
	}
}

func assertInspectionFailure(t *testing.T, result CommandResult, resultCode, stage string) {
	t.Helper()
	if result.Status != "failed" || result.ResultCode != resultCode || result.Failure == nil || result.Failure.Stage != stage || result.Failure.Detail == "" {
		t.Fatalf("result: %+v", result)
	}
}

const successfulMySQLTool = `#!/bin/sh
if [ "$1" = "--version" ]; then
    echo "mysql synthetic-version"
    exit 0
fi
printf '73796e7468657469635f617070\n'
`

const failingMySQLTool = `#!/bin/sh
if [ "$1" = "--version" ]; then
    echo "mysql synthetic-version"
    exit 0
fi
printf 'Access denied for synthetic-secret\nCheck grants\n' >&2
exit 1
`

const successfulMySQLDumpTool = `#!/bin/sh
if [ "$1" = "--version" ]; then
    echo "mysqldump synthetic-version"
fi
exit 0
`

const failingMySQLDumpTool = `#!/bin/sh
if [ "$1" = "--version" ]; then
    echo "mysqldump synthetic-version"
    exit 0
fi
if [ "$1" = "--help" ]; then
    exit 0
fi
printf 'Dump denied for synthetic-secret\n' >&2
exit 1
`
