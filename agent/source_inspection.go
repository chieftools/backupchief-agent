package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"unicode"
)

func inspectSource(ctx context.Context, generation uint64, runID, stateDirectory string, payload CommandPayload) CommandResult {
	result := CommandResult{
		Generation: generation,
		RunID:      runID,
		Status:     "failed",
		ResultCode: "execution_failed",
		Summary:    "The source inspection failed.",
		Failure:    inspectionFailure("unknown", "The inspection ended before a more specific cause was recorded."),
	}
	encoded, err := json.Marshal(payload.Source)
	if err != nil {
		result.Failure = inspectionFailure("source_validation", fmt.Sprintf("encode source payload: %v", err))
		return result
	}
	var source sourceDocument
	if err := json.Unmarshal(encoded, &source); err != nil {
		result.Failure = inspectionFailure("source_validation", fmt.Sprintf("decode source payload: %v", err))
		return result
	}
	if payload.Type == "file" {
		info, err := os.Lstat(source.Root)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			result.ResultCode = "invalid_root"
			result.Summary = "The source root is missing, inaccessible, not a directory, or a symlink."
			detail := fmt.Sprintf("inspect source root %q: it is not a directory or is a symlink", source.Root)
			if err != nil {
				detail = fmt.Sprintf("inspect source root %q: %v", source.Root, err)
			}
			result.Failure = inspectionFailure("source_access", detail)
			return result
		}
		result.Status = "complete"
		result.ResultCode = "success"
		result.Summary = "The source directory is available."
		result.Failure = nil
		return result
	}
	jobType := JobType(payload.Type)
	if isPostgreSQLJob(jobType) {
		if source.Selection == nil || !validPostgreSQLJobSource(jobType, &PostgreSQLSource{SelectionMode: source.Selection.Mode, TableSelection: normalizeTableSelection(source.TableSelection)}) {
			result.Failure = inspectionFailure("source_validation", "The source type does not match its database selection fields.", source.Password)
			return result
		}
		return inspectPostgreSQLSource(ctx, result, source, stateDirectory)
	}
	if !isMySQLJob(jobType) || source.Selection == nil || source.Dump == nil || !validMySQLJobSource(jobType, &MySQLSource{SelectionMode: source.Selection.Mode, TableSelection: normalizeTableSelection(source.TableSelection)}) {
		result.Failure = inspectionFailure("source_validation", "The source type or required source fields are not supported by this agent.")
		return result
	}
	mysql := MySQLSource{
		Host: source.Host, Port: source.Port, Username: source.Username, Password: source.Password,
		SelectionMode: source.Selection.Mode, Databases: source.Selection.Databases,
		IncludeRoutines: source.Dump.IncludeRoutines, IncludeEvents: source.Dump.IncludeEvents,
		CustomFlags:    source.Dump.CustomFlags,
		TableSelection: normalizeTableSelection(source.TableSelection),
	}
	if err := validateMySQLSource(JobSource{MySQL: &mysql}); err != nil {
		result.Summary = "The MySQL source configuration is invalid."
		result.Failure = inspectionFailure("source_validation", err.Error(), mysql.Password)
		return result
	}
	mysqlBinary, mysqlErr := resolveExternalTool("mysql")
	dumpBinary, dumpErr := resolveExternalTool("mysqldump")
	if mysqlErr != nil || dumpErr != nil {
		result.ResultCode = "tool_unavailable"
		result.Summary = "Executable mysql and mysqldump client tools are required."
		result.Tools = map[string]any{"mysql": probeExternalTool("mysql"), "mysqldump": probeExternalTool("mysqldump")}
		result.Failure = inspectionFailure("tool_check", toolResolutionDetail(mysqlErr, dumpErr))
		return result
	}
	optionFile, cleanup, err := mysqlOptionFile(mysql, stateDirectory)
	if err != nil {
		result.Failure = inspectionFailure("credential_setup", fmt.Sprintf("prepare private MySQL credentials: %v", err), mysql.Password)
		return result
	}
	defer cleanup()
	discovered, err := discoverMySQLDatabases(ctx, mysqlBinary, optionFile)
	if err != nil {
		result.ResultCode = "source_authentication_failed"
		result.Summary = "MySQL rejected the connection or database discovery query."
		result.Failure = inspectionFailure("database_discovery", err.Error(), mysql.Password)
		return result
	}
	targets, valid := selectedMySQLDatabases(mysql, discovered)
	if !valid {
		result.ResultCode = "database_selection_invalid"
		result.Summary = "The MySQL database selection is invalid."
		result.Databases = discovered
		result.Failure = inspectionFailure("database_selection", "The selection contains a database that was not returned as accessible, or no accessible databases remain after exclusions.", mysql.Password)
		return result
	}
	testDatabase := targets[0]
	if mysql.TableSelection != nil {
		accessible := map[string]bool{}
		for _, database := range targets {
			accessible[database] = true
		}
		tables, catalogErr := discoverMySQLTables(ctx, mysqlBinary, optionFile)
		if catalogErr != nil {
			result.ResultCode = "table_selection_invalid"
			result.Summary = "The selected MySQL tables could not be verified."
			result.Databases = discovered
			result.Failure = inspectionFailure("table_selection", catalogErr.Error(), mysql.Password)
			return result
		}
		for _, table := range mysql.TableSelection.Tables {
			if !accessible[table.Database] || !tables[table.Database+"\x00"+table.Table] {
				result.ResultCode = "table_selection_invalid"
				result.Summary = "The selected MySQL tables do not all exist or are not accessible."
				result.Databases = discovered
				result.Failure = inspectionFailure("table_selection", fmt.Sprintf("Table %q in database %q was not returned by the catalog query.", table.Table, table.Database), mysql.Password)
				return result
			}
		}
	}
	arguments := []string{"--defaults-extra-file=" + optionFile, "--no-data", "--single-transaction", "--quick", "--skip-lock-tables", "--no-tablespaces"}
	if mysqlDumpSupportsColumnStatistics(ctx, dumpBinary) {
		arguments = append(arguments, "--column-statistics=0")
	}
	arguments = append(arguments, mysql.CustomFlags...)
	arguments = append(arguments, mysqlTableArguments(mysql, testDatabase)...)
	command := exec.CommandContext(ctx, dumpBinary, arguments...)
	command.Env = []string{"PATH=/usr/bin:/bin:/usr/local/bin:/usr/local/mysql/bin", "LANG=C"}
	command.Stdout = &bytes.Buffer{}
	var stderr inspectionOutput
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		result.ResultCode = "source_authentication_failed"
		result.Summary = "mysqldump could not produce a schema-only test dump."
		result.Databases = discovered
		result.Failure = inspectionFailure("dump_test", commandFailure("mysqldump", err, stderr.String()).Error(), mysql.Password)
		return result
	}
	result.Status = "complete"
	result.ResultCode = "success"
	result.Summary = "MySQL credentials and a schema-only dump were verified."
	result.Databases = discovered
	result.Tools = map[string]any{"mysql": probeExternalTool("mysql"), "mysqldump": probeExternalTool("mysqldump")}
	result.Failure = nil
	return result
}

func inspectPostgreSQLSource(ctx context.Context, result CommandResult, source sourceDocument, stateDirectory string) CommandResult {
	if source.Selection == nil || source.Dump != nil {
		result.Failure = inspectionFailure("source_validation", "The PostgreSQL source fields are incomplete.", source.Password)
		return result
	}
	postgresql := PostgreSQLSource{
		Host: source.Host, Port: source.Port, Username: source.Username, Password: source.Password,
		ConnectionDatabase: source.ConnectionDatabase, SelectionMode: source.Selection.Mode, Databases: source.Selection.Databases,
		TableSelection: normalizeTableSelection(source.TableSelection),
	}
	if err := validatePostgreSQLSource(JobSource{PostgreSQL: &postgresql}); err != nil {
		result.Summary = "The PostgreSQL source configuration is invalid."
		result.Failure = inspectionFailure("source_validation", err.Error(), postgresql.Password)
		return result
	}
	psqlBinary, psqlErr := resolveExternalTool("psql")
	dumpBinary, dumpErr := resolveExternalTool("pg_dump")
	if psqlErr != nil || dumpErr != nil {
		result.ResultCode = "tool_unavailable"
		result.Summary = "Executable psql and pg_dump client tools are required."
		result.Tools = map[string]any{"psql": probeExternalTool("psql"), "pg_dump": probeExternalTool("pg_dump")}
		result.Failure = inspectionFailure("tool_check", postgresqlToolResolutionDetail(psqlErr, dumpErr))
		return result
	}
	passfile, cleanup, err := postgresqlPassfile(postgresql, postgresql.ConnectionDatabase, stateDirectory)
	if err != nil {
		result.Failure = inspectionFailure("credential_setup", fmt.Sprintf("prepare private PostgreSQL credentials: %v", err), postgresql.Password)
		return result
	}
	discovered, err := discoverPostgreSQLDatabases(ctx, psqlBinary, postgresql, passfile)
	cleanup()
	if err != nil {
		result.ResultCode = "source_authentication_failed"
		result.Summary = "PostgreSQL rejected the connection or database discovery query."
		result.Failure = inspectionFailure("database_discovery", err.Error(), postgresql.Password)
		return result
	}
	targets, valid := selectedPostgreSQLDatabases(postgresql, discovered)
	if !valid {
		result.ResultCode = "database_selection_invalid"
		result.Summary = "The selected PostgreSQL databases are not accessible."
		result.Databases = discovered
		result.Failure = inspectionFailure("database_selection", "The selection contains a database that was not returned as connectable, or no connectable databases were found.", postgresql.Password)
		return result
	}
	if postgresql.TableSelection != nil {
		catalogs := map[string]map[string]bool{}
		for _, table := range postgresql.TableSelection.Tables {
			if _, checked := catalogs[table.Database]; checked {
				continue
			}
			tablePassfile, tableCleanup, passfileErr := postgresqlPassfile(postgresql, table.Database, stateDirectory)
			if passfileErr != nil {
				result.Failure = inspectionFailure("credential_setup", fmt.Sprintf("prepare private PostgreSQL credentials: %v", passfileErr), postgresql.Password)
				return result
			}
			catalog, catalogErr := discoverPostgreSQLTables(ctx, psqlBinary, postgresql, table.Database, tablePassfile)
			tableCleanup()
			if catalogErr != nil {
				result.ResultCode = "table_selection_invalid"
				result.Summary = "The selected PostgreSQL tables could not be verified."
				result.Databases = discovered
				result.Failure = inspectionFailure("table_selection", catalogErr.Error(), postgresql.Password)
				return result
			}
			catalogs[table.Database] = catalog
		}
		for _, table := range postgresql.TableSelection.Tables {
			if !catalogs[table.Database][table.Schema+"\x00"+table.Table] {
				result.ResultCode = "table_selection_invalid"
				result.Summary = "The selected PostgreSQL tables do not all exist or are not accessible."
				result.Databases = discovered
				result.Failure = inspectionFailure("table_selection", fmt.Sprintf("Table %q in schema %q of database %q was not returned by the catalog query.", table.Table, table.Schema, table.Database), postgresql.Password)
				return result
			}
		}
	}

	testDatabase := targets[0]
	passfile, cleanup, err = postgresqlPassfile(postgresql, testDatabase, stateDirectory)
	if err != nil {
		result.Failure = inspectionFailure("credential_setup", fmt.Sprintf("prepare private PostgreSQL credentials: %v", err), postgresql.Password)
		return result
	}
	defer cleanup()
	command := exec.CommandContext(ctx, dumpBinary, postgresqlDumpArguments(postgresql, testDatabase, passfile, true)...)
	command.Env = []string{"PATH=/usr/bin:/bin:/usr/local/bin", "LANG=C"}
	command.Stdout = &bytes.Buffer{}
	var stderr inspectionOutput
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		result.ResultCode = "execution_failed"
		result.Summary = "pg_dump could not produce a schema-only test dump."
		result.Databases = discovered
		result.Failure = inspectionFailure("dump_execution", commandFailure("pg_dump", err, stderr.String()).Error(), postgresql.Password)
		return result
	}
	result.Status = "complete"
	result.ResultCode = "success"
	result.Summary = "PostgreSQL credentials and a schema-only dump were verified."
	result.Databases = discovered
	result.Tools = map[string]any{"psql": probeExternalTool("psql"), "pg_dump": probeExternalTool("pg_dump")}
	result.Failure = nil
	return result
}

func inspectionFailure(stage, detail string, secrets ...string) *SourceInspectionFailure {
	for _, secret := range secrets {
		if secret != "" {
			detail = strings.ReplaceAll(detail, secret, "[redacted]")
		}
	}
	detail = strings.Map(func(value rune) rune {
		if unicode.IsControl(value) {
			return ' '
		}
		return value
	}, detail)
	detail = strings.Join(strings.Fields(detail), " ")
	if detail == "" {
		detail = "No technical detail was returned."
	}
	runes := []rune(detail)
	if len(runes) > 1000 {
		detail = string(runes[:1000])
	}

	return &SourceInspectionFailure{Stage: stage, Detail: detail}
}

func toolResolutionDetail(mysqlErr, dumpErr error) string {
	details := []string{}
	if mysqlErr != nil {
		details = append(details, "mysql: "+mysqlErr.Error())
	}
	if dumpErr != nil {
		details = append(details, "mysqldump: "+dumpErr.Error())
	}
	return strings.Join(details, "; ")
}

func postgresqlToolResolutionDetail(psqlErr, dumpErr error) string {
	details := []string{}
	if psqlErr != nil {
		details = append(details, "psql: "+psqlErr.Error())
	}
	if dumpErr != nil {
		details = append(details, "pg_dump: "+dumpErr.Error())
	}
	return strings.Join(details, "; ")
}

func logInspectionFailure(runID string, failure *SourceInspectionFailure) {
	if failure != nil {
		log.Printf("backupchief: source inspection %s failed at %s: %s", runID, failure.Stage, failure.Detail)
	}
}
