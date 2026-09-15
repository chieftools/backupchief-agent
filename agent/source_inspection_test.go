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

func TestSourceInspectionAllowsMySQLDumpWithoutGTIDOption(t *testing.T) {
	installInspectionTools(t, successfulMySQLTool, successfulMySQLDumpWithoutGTIDTool)

	result := inspectSource(context.Background(), 1, "01k4p4k2n8d3r6t9v1w5x7yabc", t.TempDir(), inspectionMySQLPayload())

	if result.Status != "complete" || result.ResultCode != "success" || result.Failure != nil {
		t.Fatalf("result: %+v", result)
	}
}

func TestSourceInspectionAppliesMySQLDatabaseExclusions(t *testing.T) {
	installInspectionTools(t, successfulMySQLTool, successfulMySQLDumpTool)
	payload := inspectionMySQLPayload()
	payload.Type = "mysql_filtered"
	payload.Source["selection"] = map[string]any{"mode": "exclude", "databases": []string{"synthetic_scratch"}}

	result := inspectSource(context.Background(), 1, "01k4p4k2n8d3r6t9v1w5x7yabc", t.TempDir(), payload)

	if result.Status != "complete" || result.ResultCode != "success" || result.Failure != nil {
		t.Fatalf("result: %+v", result)
	}
}

func TestSourceInspectionVerifiesExactMySQLTableSelection(t *testing.T) {
	installInspectionTools(t, catalogMySQLTool, successfulMySQLDumpTool)
	payload := inspectionMySQLPayload()
	payload.Type = "mysql_filtered"
	payload.Source["table_selection"] = map[string]any{
		"mode": "exclude", "tables": []map[string]any{{"database": "synthetic_app", "table": "transient_rows"}},
	}

	result := inspectSource(context.Background(), 1, "01k4p4k2n8d3r6t9v1w5x7yabc", t.TempDir(), payload)

	if result.Status != "complete" || result.ResultCode != "success" || result.Failure != nil {
		t.Fatalf("result: %+v", result)
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

func TestSourceInspectionVerifiesPostgreSQLSelectionAndSchemaDump(t *testing.T) {
	installPostgreSQLInspectionTools(t, successfulPostgreSQLTool, successfulPostgreSQLDumpTool)
	stateDirectory := t.TempDir()

	result := inspectSource(context.Background(), 1, "01k4p4k2n8d3r6t9v1w5x7yabc", stateDirectory, inspectionPostgreSQLPayload())

	if result.Status != "complete" || result.ResultCode != "success" || result.Failure != nil || len(result.Databases) != 2 || result.Databases[0] != "postgres" || result.Databases[1] != "synthetic_app" {
		t.Fatalf("result: %+v", result)
	}
	entries, err := os.ReadDir(stateDirectory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("credential workspace was not cleaned up: %v %v", entries, err)
	}
}

func TestSourceInspectionRejectsAnInaccessiblePostgreSQLSelection(t *testing.T) {
	installPostgreSQLInspectionTools(t, successfulPostgreSQLTool, successfulPostgreSQLDumpTool)
	payload := inspectionPostgreSQLPayload()
	payload.Source["selection"] = map[string]any{"mode": "selected", "databases": []string{"missing_database"}}

	result := inspectSource(context.Background(), 1, "01k4p4k2n8d3r6t9v1w5x7yabc", t.TempDir(), payload)

	assertInspectionFailure(t, result, "database_selection_invalid", "database_selection")
}

func TestSourceInspectionRejectsAMissingExactPostgreSQLTable(t *testing.T) {
	installPostgreSQLInspectionTools(t, catalogPostgreSQLTool, successfulPostgreSQLDumpTool)
	payload := inspectionPostgreSQLPayload()
	payload.Type = "postgresql_filtered"
	payload.Source["table_selection"] = map[string]any{
		"mode": "include", "tables": []map[string]any{{"database": "synthetic_app", "schema": "analytics", "table": "missing_rollup"}},
	}

	result := inspectSource(context.Background(), 1, "01k4p4k2n8d3r6t9v1w5x7yabc", t.TempDir(), payload)

	assertInspectionFailure(t, result, "table_selection_invalid", "table_selection")
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

func inspectionPostgreSQLPayload() CommandPayload {
	return CommandPayload{
		Type: "postgresql",
		Source: map[string]any{
			"host": "postgresql.example.test", "port": 5432, "username": "synthetic_reader", "password": "synthetic-secret", "connection_database": "postgres",
			"selection": map[string]any{"mode": "selected", "databases": []string{"synthetic_app"}},
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

func installPostgreSQLInspectionTools(t *testing.T, psql, pgDump string) {
	t.Helper()
	directory := t.TempDir()
	writeInspectionTool(t, directory, "psql", psql)
	writeInspectionTool(t, directory, "pg_dump", pgDump)
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

const catalogMySQLTool = `#!/bin/sh
if [ "$1" = "--version" ]; then
    echo "mysql synthetic-version"
    exit 0
fi
for argument in "$@"; do
    case "$argument" in
        *INFORMATION_SCHEMA.TABLES*)
            printf '73796e7468657469635f617070\t7472616e7369656e745f726f7773\n'
            exit 0
            ;;
    esac
done
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
    exit 0
fi
if [ "$1" = "--help" ]; then
	printf '%s\n' '  --column-statistics' '  --set-gtid-purged=name'
    exit 0
fi
for argument in "$@"; do
    if [ "$argument" = "--set-gtid-purged=OFF" ]; then
        exit 0
    fi
done
printf 'missing portable GTID option\n' >&2
exit 1
`

const successfulMySQLDumpWithoutGTIDTool = `#!/bin/sh
if [ "$1" = "--version" ]; then
    echo "mysqldump synthetic-version"
    exit 0
fi
if [ "$1" = "--help" ]; then
    echo "  --skip-lock-tables"
    exit 0
fi
for argument in "$@"; do
    if [ "$argument" = "--set-gtid-purged=OFF" ]; then
        printf 'unsupported synthetic GTID option\n' >&2
        exit 1
    fi
done
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

const successfulPostgreSQLTool = `#!/bin/sh
if [ "$1" = "--version" ]; then
    echo "psql synthetic-version"
    exit 0
fi
found_on_error_stop=false
for argument in "$@"; do
    if [ "$argument" = "--set=ON_ERROR_STOP=on" ]; then
        found_on_error_stop=true
    fi
done
if [ "$found_on_error_stop" != "true" ]; then
    printf 'missing ON_ERROR_STOP\n' >&2
    exit 1
fi
printf '706f737467726573\n73796e7468657469635f617070\n'
`

const catalogPostgreSQLTool = `#!/bin/sh
if [ "$1" = "--version" ]; then
    echo "psql synthetic-version"
    exit 0
fi
for argument in "$@"; do
    case "$argument" in
        *pg_class*)
            printf '616e616c7974696373\t6461696c795f726f6c6c7570\n'
            exit 0
            ;;
    esac
done
printf '706f737467726573\n73796e7468657469635f617070\n'
`

const successfulPostgreSQLDumpTool = `#!/bin/sh
if [ "$1" = "--version" ]; then
    echo "pg_dump synthetic-version"
fi
exit 0
`
