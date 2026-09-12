package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
)

func inspectSource(ctx context.Context, generation uint64, runID string, payload CommandPayload) CommandResult {
	result := CommandResult{
		Generation: generation,
		RunID:      runID,
		Status:     "failed",
		ResultCode: "execution_failed",
		Summary:    "The source inspection failed.",
	}
	encoded, err := json.Marshal(payload.Source)
	if err != nil {
		return result
	}
	var source sourceDocument
	if err := json.Unmarshal(encoded, &source); err != nil {
		return result
	}
	if payload.Type == "file" {
		info, err := os.Lstat(source.Root)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			result.ResultCode = "invalid_root"
			result.Summary = "The source root is missing, inaccessible, not a directory, or a symlink."
			return result
		}
		result.Status = "complete"
		result.ResultCode = "success"
		result.Summary = "The source directory is available."
		return result
	}
	if payload.Type != "mysql" || source.Selection == nil || source.Dump == nil {
		return result
	}
	mysql := MySQLSource{
		Host: source.Host, Port: source.Port, Username: source.Username, Password: source.Password,
		SelectionMode: source.Selection.Mode, Databases: source.Selection.Databases,
		IncludeRoutines: source.Dump.IncludeRoutines, IncludeEvents: source.Dump.IncludeEvents,
		CustomFlags: source.Dump.CustomFlags,
	}
	if err := validateMySQLSource(JobSource{MySQL: &mysql}); err != nil {
		result.Summary = "The MySQL source configuration is invalid."
		return result
	}
	mysqlBinary, mysqlErr := resolveExternalTool("mysql")
	dumpBinary, dumpErr := resolveExternalTool("mysqldump")
	if mysqlErr != nil || dumpErr != nil {
		result.ResultCode = "tool_unavailable"
		result.Summary = "Executable mysql and mysqldump client tools are required."
		result.Tools = map[string]any{"mysql": probeExternalTool("mysql"), "mysqldump": probeExternalTool("mysqldump")}
		return result
	}
	optionFile, cleanup, err := mysqlOptionFile(mysql)
	if err != nil {
		return result
	}
	defer cleanup()
	discovered, err := discoverMySQLDatabases(ctx, mysqlBinary, optionFile)
	if err != nil {
		result.ResultCode = "source_authentication_failed"
		result.Summary = "MySQL rejected the connection or database discovery query."
		return result
	}
	testDatabase := ""
	if mysql.SelectionMode == "selected" {
		testDatabase = mysql.Databases[0]
	} else if len(discovered) > 0 {
		testDatabase = discovered[0]
	}
	if testDatabase == "" {
		result.ResultCode = "source_authentication_failed"
		result.Summary = "No accessible databases were discovered."
		result.Databases = discovered
		return result
	}
	arguments := []string{"--defaults-extra-file=" + optionFile, "--no-data", "--single-transaction", "--quick", "--skip-lock-tables", "--no-tablespaces"}
	if mysqlDumpSupportsColumnStatistics(ctx, dumpBinary) {
		arguments = append(arguments, "--column-statistics=0")
	}
	arguments = append(arguments, mysql.CustomFlags...)
	arguments = append(arguments, "--databases", testDatabase)
	command := exec.CommandContext(ctx, dumpBinary, arguments...)
	command.Env = []string{"PATH=/usr/bin:/bin:/usr/local/bin:/usr/local/mysql/bin", "LANG=C"}
	command.Stdout = &bytes.Buffer{}
	if err := command.Run(); err != nil {
		result.ResultCode = "source_authentication_failed"
		result.Summary = "mysqldump could not produce a schema-only test dump."
		result.Databases = discovered
		return result
	}
	result.Status = "complete"
	result.ResultCode = "success"
	result.Summary = "MySQL credentials and a schema-only dump were verified."
	result.Databases = discovered
	result.Tools = map[string]any{"mysql": probeExternalTool("mysql"), "mysqldump": probeExternalTool("mysqldump")}
	return result
}
