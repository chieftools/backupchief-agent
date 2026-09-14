package agent

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/chieftools/backupchief-agent/restic"
)

var mysqlSystemDatabases = map[string]bool{
	"information_schema": true,
	"performance_schema": true,
	"sys":                true,
	"mysql":              true,
}

var portableMySQLFilenamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

func executeMySQLBackup(ctx context.Context, executor BackupExecutor, stateDirectory, serverID string, generation uint64, command *JournalCommand, job Job, now func() time.Time, progress DatabaseBackupProgress) (CommandResult, []byte, bool, uint64) {
	startedAt := now()
	result := CommandResult{Generation: generation, RunID: command.RunID, JobID: job.ID, RunKind: "backup", StartedAt: protocolTimestamp(startedAt), SnapshotIDs: []string{}, Artifacts: []BackupArtifact{}}
	mysql := job.Source.MySQL
	if mysql == nil {
		return failedMySQLResult(result, "execution_failed", "The MySQL source configuration is unavailable.", now)
	}

	mysqlBinary, mysqlErr := resolveExternalTool("mysql")
	dumpBinary, dumpErr := resolveExternalTool("mysqldump")
	if mysqlErr != nil || dumpErr != nil {
		return failedMySQLResult(result, "tool_unavailable", "The mysql and mysqldump tools are required on this server.", now)
	}

	databases := append([]string{}, mysql.Databases...)
	if mysql.SelectionMode != "selected" {
		optionFile, cleanup, err := mysqlOptionFile(*mysql, stateDirectory)
		if err != nil {
			return failedMySQLResult(result, "execution_failed", "Could not prepare private MySQL credentials.", now)
		}
		defer cleanup()

		discovered, discoveryErr := discoverMySQLDatabases(ctx, mysqlBinary, optionFile)
		if discoveryErr != nil {
			return failedMySQLResult(result, "source_authentication_failed", "Could not discover accessible MySQL databases.", now)
		}
		databases, _ = selectedMySQLDatabases(*mysql, discovered)
	}
	if len(databases) == 0 || len(databases) > maximumMySQLDatabases {
		return failedMySQLResult(result, "database_selection_invalid", "The MySQL selection must contain between 1 and 1000 databases.", now)
	}

	var retained bytes.Buffer
	var total RunStatistics = RunStatistics{"files_new": uint64(0), "files_changed": uint64(0), "files_unmodified": uint64(0), "directories_new": uint64(0), "directories_changed": uint64(0), "directories_unmodified": uint64(0), "source_files": uint64(0), "source_bytes": uint64(0), "stored_bytes": uint64(0)}
	failed := 0
	anchorWritten := false
	supportsColumnStatistics := mysqlDumpSupportsColumnStatistics(ctx, dumpBinary)
	for _, database := range databases {
		if ctx.Err() != nil {
			result.Status, result.ResultCode, result.Summary = "cancelled", "cancelled", "The MySQL backup was cancelled."
			break
		}
		encoded := hex.EncodeToString([]byte(database))
		filename := mysqlDumpFilename(database)
		flags := []string{"--defaults-extra-file={backupchief-command-config}", "--single-transaction", "--quick", "--skip-lock-tables", "--no-tablespaces"}
		if supportsColumnStatistics {
			flags = append(flags, "--column-statistics=0")
		}
		if mysql.IncludeRoutines {
			flags = append(flags, "--routines")
		}
		if mysql.IncludeEvents {
			flags = append(flags, "--events")
		}
		flags = append(flags, mysql.CustomFlags...)
		flags = append(flags, mysqlTableArguments(*mysql, database)...)
		tags := []string{"backupchief-job:" + job.ID, "backupchief-run:" + command.RunID, "backupchief-type:mysql", "backupchief-database:" + encoded}
		if !anchorWritten {
			tags = append(tags, "backupchief-run-anchor")
		}
		request := restic.Request{
			Version: 1, Operation: "backup_stdin", Connection: resticConnection(job), Password: job.Repository.ServicePassword,
			Host: serverID, Tags: tags, StdinFilename: filename, StdinCommand: append([]string{dumpBinary}, flags...), CommandConfig: mysqlOptionContents(*mysql), TimeoutSeconds: 12 * 60 * 60, LockWaitSeconds: 5 * 60,
		}
		resticResult := executor.Run(ctx, request)
		log, _, _ := retainedLog(resticResult)
		retained.WriteString("database " + database + ":\n")
		retained.Write(log)
		summaries := parseResticSummaries(resticResult.Output)
		if resticResult.ExitCode != 0 || len(summaries) == 0 || !digestPattern.MatchString(summaries[len(summaries)-1].SnapshotID) {
			failed++
			if isRepositoryFailure(resticResult.ExitCode) {
				result.Status, result.ResultCode, result.Summary = "failed", resultCodeForExit(resticResult.ExitCode), "The repository stopped the MySQL backup."
				break
			}
			continue
		}
		summary := summaries[len(summaries)-1]
		artifact := BackupArtifact{Database: database, Filename: filename, SnapshotID: summary.SnapshotID, SourceBytes: &summary.SourceBytes, StoredBytes: &summary.StoredBytes}
		result.SnapshotIDs = append(result.SnapshotIDs, summary.SnapshotID)
		result.Artifacts = append(result.Artifacts, artifact)
		addDatabaseStatistics(total, summary)
		anchorWritten = true
		if progress != nil {
			progress(artifact, len(result.Artifacts), len(databases))
		}
	}

	result.FinishedAt = protocolTimestamp(now())
	result.Statistics = &total
	if result.Status == "" {
		switch {
		case len(result.SnapshotIDs) == 0:
			result.Status, result.ResultCode, result.Summary = "failed", "execution_failed", "No MySQL database snapshot completed."
		case failed > 0:
			result.Status, result.ResultCode, result.Summary = "partial", "databases_failed", fmt.Sprintf("Backed up %d database(s); %d failed.", len(result.SnapshotIDs), failed)
		default:
			result.Status, result.ResultCode, result.Summary = "complete", "success", fmt.Sprintf("Backed up %d MySQL database(s).", len(result.SnapshotIDs))
		}
	}
	log := retained.Bytes()
	truncated := false
	dropped := uint64(0)
	if len(log) > maximumRunLog {
		dropped = uint64(len(log) - maximumRunLog)
		log = log[:maximumRunLog]
		truncated = true
	}
	return result, append([]byte(nil), log...), truncated, dropped
}

func selectedMySQLDatabases(source MySQLSource, discovered []string) ([]string, bool) {
	if source.SelectionMode == "all_accessible" {
		return append([]string(nil), discovered...), len(discovered) > 0 && len(discovered) <= maximumMySQLDatabases
	}
	if source.SelectionMode == "selected" {
		accessible := make(map[string]bool, len(discovered))
		for _, database := range discovered {
			accessible[database] = true
		}
		databases := append([]string(nil), source.Databases...)
		sort.Strings(databases)
		for _, database := range databases {
			if !accessible[database] {
				return nil, false
			}
		}
		return databases, len(databases) > 0 && len(databases) <= maximumMySQLDatabases
	}

	excluded := make(map[string]bool, len(source.Databases))
	for _, database := range source.Databases {
		excluded[database] = true
	}
	targets := make([]string, 0, len(discovered))
	for _, database := range discovered {
		if !excluded[database] {
			targets = append(targets, database)
		}
	}
	return targets, len(targets) > 0 && len(targets) <= maximumMySQLDatabases
}

func mysqlTableArguments(source MySQLSource, database string) []string {
	selection := source.TableSelection
	if selection == nil {
		return []string{"--databases", database}
	}

	tables := make([]string, 0)
	for _, table := range selection.Tables {
		if table.Database == database {
			tables = append(tables, table.Table)
		}
	}
	sort.Strings(tables)
	if selection.Mode == "include" {
		return append([]string{database}, tables...)
	}

	arguments := make([]string, 0, len(tables)+2)
	for _, table := range tables {
		arguments = append(arguments, "--ignore-table="+database+"."+table)
	}
	return append(arguments, "--databases", database)
}

func mysqlDumpFilename(database string) string {
	if portableMySQLFilenamePattern.MatchString(database) {
		return database + ".sql"
	}

	digest := sha256.Sum256([]byte(database))
	return "~" + hex.EncodeToString(digest[:]) + ".sql"
}

func failedMySQLResult(result CommandResult, code, summary string, now func() time.Time) (CommandResult, []byte, bool, uint64) {
	result.Status, result.ResultCode, result.Summary, result.FinishedAt = "failed", code, summary, protocolTimestamp(now())
	return result, []byte(summary + "\n"), false, 0
}

func mysqlOptionContents(source MySQLSource) string {
	escape := func(value string) string {
		return strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\r", "\\r", "\"", "\\\"").Replace(value)
	}
	return "[client]\nhost=\"" + escape(source.Host) + "\"\nport=" + strconv.Itoa(int(source.Port)) + "\nuser=\"" + escape(source.Username) + "\"\npassword=\"" + escape(source.Password) + "\"\nprotocol=tcp\n"
}

func mysqlOptionFile(source MySQLSource, stateDirectory string) (string, func(), error) {
	directory, err := os.MkdirTemp(stateDirectory, "mysql-")
	if err != nil {
		return "", func() {}, err
	}
	if err = os.Chmod(directory, 0700); err != nil {
		_ = os.RemoveAll(directory)
		return "", func() {}, err
	}
	path := filepath.Join(directory, "client.cnf")
	if err = os.WriteFile(path, []byte(mysqlOptionContents(source)), 0600); err != nil {
		_ = os.RemoveAll(directory)
		return "", func() {}, err
	}
	return path, func() { _ = os.RemoveAll(directory) }, nil
}

func discoverMySQLDatabases(ctx context.Context, binary, optionFile string) ([]string, error) {
	query := "SELECT HEX(SCHEMA_NAME) FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME"
	command := exec.CommandContext(ctx, binary, "--defaults-extra-file="+optionFile, "--batch", "--skip-column-names", "--execute="+query)
	command.Env = []string{"PATH=/usr/bin:/bin:/usr/local/bin:/usr/local/mysql/bin", "LANG=C"}
	var output bytes.Buffer
	var stderr inspectionOutput
	command.Stdout = &output
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return nil, commandFailure("mysql", err, stderr.String())
	}
	databases := []string{}
	scanner := bufio.NewScanner(bytes.NewReader(output.Bytes()))
	for scanner.Scan() {
		decoded, decodeErr := hex.DecodeString(strings.TrimSpace(scanner.Text()))
		if decodeErr != nil {
			return nil, decodeErr
		}
		name := string(decoded)
		if !mysqlSystemDatabases[strings.ToLower(name)] {
			databases = append(databases, name)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	sort.Strings(databases)
	return databases, nil
}

func discoverMySQLTables(ctx context.Context, binary, optionFile string) (map[string]bool, error) {
	query := "SELECT HEX(TABLE_SCHEMA), HEX(TABLE_NAME) FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_SCHEMA, TABLE_NAME"
	command := exec.CommandContext(ctx, binary, "--defaults-extra-file="+optionFile, "--batch", "--skip-column-names", "--execute="+query)
	command.Env = []string{"PATH=/usr/bin:/bin:/usr/local/bin:/usr/local/mysql/bin", "LANG=C"}
	var output bytes.Buffer
	var stderr inspectionOutput
	command.Stdout = &output
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return nil, commandFailure("mysql", err, stderr.String())
	}

	tables := map[string]bool{}
	scanner := bufio.NewScanner(bytes.NewReader(output.Bytes()))
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), "\t")
		if len(parts) != 2 {
			return nil, fmt.Errorf("mysql returned an invalid table catalog row")
		}
		database, databaseErr := hex.DecodeString(strings.TrimSpace(parts[0]))
		table, tableErr := hex.DecodeString(strings.TrimSpace(parts[1]))
		if databaseErr != nil || tableErr != nil {
			return nil, fmt.Errorf("mysql returned an invalid table catalog row")
		}
		tables[string(database)+"\x00"+string(table)] = true
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return tables, nil
}

type inspectionOutput struct {
	value []byte
}

func (output *inspectionOutput) Write(value []byte) (int, error) {
	const maximum = 8 << 10

	remaining := maximum - len(output.value)
	if remaining > 0 {
		if len(value) < remaining {
			remaining = len(value)
		}
		output.value = append(output.value, value[:remaining]...)
	}

	return len(value), nil
}

func (output *inspectionOutput) String() string {
	return string(output.value)
}

func commandFailure(tool string, err error, stderr string) error {
	if detail := strings.TrimSpace(stderr); detail != "" {
		return fmt.Errorf("%s: %s", tool, detail)
	}

	return fmt.Errorf("%s: %w", tool, err)
}

func mysqlDumpSupportsColumnStatistics(ctx context.Context, binary string) bool {
	probe, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	output, err := exec.CommandContext(probe, binary, "--help").CombinedOutput()
	return err == nil && bytes.Contains(output, []byte("column-statistics"))
}

func resticConnection(job Job) restic.Connection {
	connection := job.Repository.Connection
	return restic.Connection{Driver: connection.Driver, Path: connection.Path, Endpoint: connection.Endpoint, Bucket: connection.Bucket, Prefix: connection.Prefix, Region: connection.Region, AccessKey: connection.AccessKey, SecretKey: connection.SecretKey}
}

func addDatabaseStatistics(total RunStatistics, summary resticSummary) {
	values := map[string]uint64{"files_new": summary.FilesNew, "files_changed": summary.FilesChanged, "files_unmodified": summary.FilesUnmodified, "directories_new": summary.DirectoriesNew, "directories_changed": summary.DirectoriesChanged, "directories_unmodified": summary.DirectoriesUnmodified, "source_files": summary.SourceFiles, "source_bytes": summary.SourceBytes, "stored_bytes": summary.StoredBytes}
	for key, value := range values {
		total[key] = total[key].(uint64) + value
	}
}

func isRepositoryFailure(code int) bool { return code == 10 || code == 11 || code == 12 }
