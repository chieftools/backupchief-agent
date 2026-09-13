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

var portablePostgreSQLFilenamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

func executePostgreSQLBackup(ctx context.Context, executor BackupExecutor, stateDirectory, serverID string, generation uint64, command *JournalCommand, job Job, now func() time.Time) (CommandResult, []byte, bool, uint64) {
	startedAt := now()
	result := CommandResult{Generation: generation, RunID: command.RunID, JobID: job.ID, RunKind: "backup", StartedAt: protocolTimestamp(startedAt), SnapshotIDs: []string{}, Artifacts: []BackupArtifact{}}
	postgresql := job.Source.PostgreSQL
	if postgresql == nil {
		return failedPostgreSQLResult(result, "execution_failed", "The PostgreSQL source configuration is unavailable.", now)
	}

	psqlBinary, psqlErr := resolveExternalTool("psql")
	dumpBinary, dumpErr := resolveExternalTool("pg_dump")
	if psqlErr != nil || dumpErr != nil {
		return failedPostgreSQLResult(result, "tool_unavailable", "The psql and pg_dump tools are required on this server.", now)
	}

	passfile, cleanup, err := postgresqlPassfile(*postgresql, postgresql.ConnectionDatabase, stateDirectory)
	if err != nil {
		return failedPostgreSQLResult(result, "execution_failed", "Could not prepare private PostgreSQL credentials.", now)
	}
	discovered, err := discoverPostgreSQLDatabases(ctx, psqlBinary, *postgresql, passfile)
	cleanup()
	if err != nil {
		return failedPostgreSQLResult(result, "source_authentication_failed", "Could not discover accessible PostgreSQL databases.", now)
	}

	databases, valid := selectedPostgreSQLDatabases(*postgresql, discovered)
	if !valid {
		return failedPostgreSQLResult(result, "database_selection_invalid", "The PostgreSQL selection must contain accessible non-template databases.", now)
	}

	var retained bytes.Buffer
	var total RunStatistics = RunStatistics{"files_new": uint64(0), "files_changed": uint64(0), "files_unmodified": uint64(0), "directories_new": uint64(0), "directories_changed": uint64(0), "directories_unmodified": uint64(0), "source_files": uint64(0), "source_bytes": uint64(0), "stored_bytes": uint64(0)}
	failed := 0
	anchorWritten := false
	for _, database := range databases {
		if ctx.Err() != nil {
			result.Status, result.ResultCode, result.Summary = "cancelled", "cancelled", "The PostgreSQL backup was cancelled."
			break
		}

		encoded := hex.EncodeToString([]byte(database))
		filename := postgresqlDumpFilename(database)
		tags := []string{"backupchief-job:" + job.ID, "backupchief-run:" + command.RunID, "backupchief-type:postgresql", "backupchief-database:" + encoded}
		if !anchorWritten {
			tags = append(tags, "backupchief-run-anchor")
		}
		request := restic.Request{
			Version: 1, Operation: "backup_stdin", Connection: resticConnection(job), Password: job.Repository.ServicePassword,
			Host: serverID, Tags: tags, StdinFilename: filename,
			StdinCommand:  append([]string{dumpBinary}, postgresqlDumpArguments(*postgresql, database, "{backupchief-command-config}", false)...),
			CommandConfig: postgresqlPassfileContents(*postgresql, database), TimeoutSeconds: 12 * 60 * 60, LockWaitSeconds: 5 * 60,
		}
		resticResult := executor.Run(ctx, request)
		log, _, _ := retainedLog(resticResult)
		retained.WriteString("database " + database + ":\n")
		retained.Write(log)
		summaries := parseResticSummaries(resticResult.Output)
		if resticResult.ExitCode != 0 || len(summaries) == 0 || !digestPattern.MatchString(summaries[len(summaries)-1].SnapshotID) {
			failed++
			if isRepositoryFailure(resticResult.ExitCode) {
				result.Status, result.ResultCode, result.Summary = "failed", resultCodeForExit(resticResult.ExitCode), "The repository stopped the PostgreSQL backup."
				break
			}
			continue
		}

		summary := summaries[len(summaries)-1]
		result.SnapshotIDs = append(result.SnapshotIDs, summary.SnapshotID)
		result.Artifacts = append(result.Artifacts, BackupArtifact{Database: database, Filename: filename, SnapshotID: summary.SnapshotID})
		addDatabaseStatistics(total, summary)
		anchorWritten = true
	}

	result.FinishedAt = protocolTimestamp(now())
	result.Statistics = &total
	if result.Status == "" {
		switch {
		case len(result.SnapshotIDs) == 0:
			result.Status, result.ResultCode, result.Summary = "failed", "execution_failed", "No PostgreSQL database snapshot completed."
		case failed > 0:
			result.Status, result.ResultCode, result.Summary = "partial", "databases_failed", fmt.Sprintf("Backed up %d database(s); %d failed.", len(result.SnapshotIDs), failed)
		default:
			result.Status, result.ResultCode, result.Summary = "complete", "success", fmt.Sprintf("Backed up %d PostgreSQL database(s).", len(result.SnapshotIDs))
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

func failedPostgreSQLResult(result CommandResult, code, summary string, now func() time.Time) (CommandResult, []byte, bool, uint64) {
	result.Status, result.ResultCode, result.Summary, result.FinishedAt = "failed", code, summary, protocolTimestamp(now())
	return result, []byte(summary + "\n"), false, 0
}

func postgresqlDumpFilename(database string) string {
	if portablePostgreSQLFilenamePattern.MatchString(database) {
		return database + ".sql"
	}

	digest := sha256.Sum256([]byte(database))
	return "~" + hex.EncodeToString(digest[:]) + ".sql"
}

func postgresqlDumpArguments(source PostgreSQLSource, database, passfile string, schemaOnly bool) []string {
	arguments := []string{
		"--format=plain",
		"--no-owner",
		"--no-privileges",
		"--no-tablespaces",
		"--quote-all-identifiers",
		"--no-password",
		"--lock-wait-timeout=300000",
	}
	if schemaOnly {
		arguments = append(arguments, "--schema-only")
	}
	return append(arguments, "--dbname="+postgresqlConnectionString(source, database, passfile))
}

func postgresqlConnectionString(source PostgreSQLSource, database, passfile string) string {
	values := []string{
		"host=" + postgresqlConnectionValue(source.Host),
		"port=" + postgresqlConnectionValue(strconv.Itoa(int(source.Port))),
		"user=" + postgresqlConnectionValue(source.Username),
		"dbname=" + postgresqlConnectionValue(database),
		"passfile=" + postgresqlConnectionValue(passfile),
		"connect_timeout=15",
		"application_name=backupchief-agent",
	}
	return strings.Join(values, " ")
}

func postgresqlConnectionValue(value string) string {
	return "'" + strings.NewReplacer("\\", "\\\\", "'", "\\'").Replace(value) + "'"
}

func postgresqlPassfileContents(source PostgreSQLSource, database string) string {
	escape := func(value string) string {
		return strings.NewReplacer("\\", "\\\\", ":", "\\:").Replace(value)
	}
	return escape(source.Host) + ":" + strconv.Itoa(int(source.Port)) + ":" + escape(database) + ":" + escape(source.Username) + ":" + escape(source.Password) + "\n"
}

func postgresqlPassfile(source PostgreSQLSource, database, stateDirectory string) (string, func(), error) {
	directory, err := os.MkdirTemp(stateDirectory, "postgresql-")
	if err != nil {
		return "", func() {}, err
	}
	if err = os.Chmod(directory, 0700); err != nil {
		_ = os.RemoveAll(directory)
		return "", func() {}, err
	}
	path := filepath.Join(directory, "pgpass")
	if err = os.WriteFile(path, []byte(postgresqlPassfileContents(source, database)), 0600); err != nil {
		_ = os.RemoveAll(directory)
		return "", func() {}, err
	}
	return path, func() { _ = os.RemoveAll(directory) }, nil
}

func discoverPostgreSQLDatabases(ctx context.Context, binary string, source PostgreSQLSource, passfile string) ([]string, error) {
	query := "SELECT encode(convert_to(datname, 'UTF8'), 'hex') FROM pg_database WHERE datallowconn AND NOT datistemplate AND has_database_privilege(datname, 'CONNECT') ORDER BY datname"
	arguments := []string{"--no-psqlrc", "--no-password", "--tuples-only", "--no-align", "--quiet", "--dbname=" + postgresqlConnectionString(source, source.ConnectionDatabase, passfile), "--command=" + query}
	command := exec.CommandContext(ctx, binary, arguments...)
	command.Env = []string{"PATH=/usr/bin:/bin:/usr/local/bin", "LANG=C"}
	var output bytes.Buffer
	var stderr inspectionOutput
	command.Stdout = &output
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return nil, commandFailure("psql", err, stderr.String())
	}

	databases := []string{}
	scanner := bufio.NewScanner(bytes.NewReader(output.Bytes()))
	for scanner.Scan() {
		decoded, err := hex.DecodeString(strings.TrimSpace(scanner.Text()))
		if err != nil {
			return nil, err
		}
		if len(decoded) > 0 {
			databases = append(databases, string(decoded))
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	sort.Strings(databases)
	return databases, nil
}

func selectedPostgreSQLDatabases(source PostgreSQLSource, discovered []string) ([]string, bool) {
	if source.SelectionMode == "all_accessible" {
		return discovered, len(discovered) > 0 && len(discovered) <= maximumPostgreSQLDatabases
	}

	accessible := map[string]bool{}
	for _, database := range discovered {
		accessible[database] = true
	}
	databases := append([]string(nil), source.Databases...)
	sort.Strings(databases)
	if len(databases) == 0 || len(databases) > maximumPostgreSQLDatabases {
		return nil, false
	}
	for _, database := range databases {
		if !accessible[database] {
			return nil, false
		}
	}
	return databases, true
}
