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

func executeMySQLBackup(ctx context.Context, executor BackupExecutor, stateDirectory, serverID string, generation uint64, command *JournalCommand, job Job, now func() time.Time) (CommandResult, []byte, bool, uint64) {
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
	if mysql.SelectionMode == "all_accessible" {
		optionFile, cleanup, err := mysqlOptionFile(*mysql, stateDirectory)
		if err != nil {
			return failedMySQLResult(result, "execution_failed", "Could not prepare private MySQL credentials.", now)
		}
		defer cleanup()

		databases, err = discoverMySQLDatabases(ctx, mysqlBinary, optionFile)
		if err != nil {
			return failedMySQLResult(result, "source_authentication_failed", "Could not discover accessible MySQL databases.", now)
		}
	}
	if len(databases) == 0 || len(databases) > maximumMySQLDatabases {
		return failedMySQLResult(result, "database_selection_invalid", "The MySQL selection must contain between 1 and 1000 databases.", now)
	}

	var retained bytes.Buffer
	var total RunStatistics = RunStatistics{"files_new": uint64(0), "files_changed": uint64(0), "files_unmodified": uint64(0), "directories_new": uint64(0), "directories_changed": uint64(0), "directories_unmodified": uint64(0), "source_files": uint64(0), "source_bytes": uint64(0), "stored_bytes": uint64(0)}
	failed := 0
	supportsColumnStatistics := mysqlDumpSupportsColumnStatistics(ctx, dumpBinary)
	for index, database := range databases {
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
		flags = append(flags, "--databases", database)
		tags := []string{"backupchief-job:" + job.ID, "backupchief-run:" + command.RunID, "backupchief-type:mysql", "backupchief-database:" + encoded}
		if index == 0 {
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
		result.SnapshotIDs = append(result.SnapshotIDs, summary.SnapshotID)
		result.Artifacts = append(result.Artifacts, BackupArtifact{Database: database, Filename: filename, SnapshotID: summary.SnapshotID})
		addMySQLStatistics(total, summary)
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

func addMySQLStatistics(total RunStatistics, summary resticSummary) {
	values := map[string]uint64{"files_new": summary.FilesNew, "files_changed": summary.FilesChanged, "files_unmodified": summary.FilesUnmodified, "directories_new": summary.DirectoriesNew, "directories_changed": summary.DirectoriesChanged, "directories_unmodified": summary.DirectoriesUnmodified, "source_files": summary.SourceFiles, "source_bytes": summary.SourceBytes, "stored_bytes": summary.StoredBytes}
	for key, value := range values {
		total[key] = total[key].(uint64) + value
	}
}

func isRepositoryFailure(code int) bool { return code == 10 || code == 11 || code == 12 }
