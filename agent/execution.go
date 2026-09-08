package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/chieftools/backupchief-agent/restic"
)

type BackupExecutor interface {
	Run(context.Context, restic.Request) restic.Result
}

type resticSummary struct {
	MessageType           string `json:"message_type"`
	FilesNew              uint64 `json:"files_new"`
	FilesChanged          uint64 `json:"files_changed"`
	FilesUnmodified       uint64 `json:"files_unmodified"`
	DirectoriesNew        uint64 `json:"dirs_new"`
	DirectoriesChanged    uint64 `json:"dirs_changed"`
	DirectoriesUnmodified uint64 `json:"dirs_unmodified"`
	SourceFiles           uint64 `json:"total_files_processed"`
	SourceBytes           uint64 `json:"total_bytes_processed"`
	StoredBytes           uint64 `json:"data_added_packed"`
	SnapshotID            string `json:"snapshot_id"`
}

type resticRepositoryStats struct {
	TotalSize uint64 `json:"total_size"`
}

func executeBackup(
	ctx context.Context,
	executor BackupExecutor,
	serverID string,
	generation uint64,
	command *JournalCommand,
	job Job,
	now func() time.Time,
) (CommandResult, []byte, bool, uint64) {
	startedAt := now()
	base := CommandResult{
		Generation:  generation,
		RunID:       command.RunID,
		JobID:       job.ID,
		RunKind:     "backup",
		StartedAt:   protocolTimestamp(startedAt),
		SnapshotIDs: []string{},
	}

	info, err := os.Lstat(job.Source.Root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		base.Status = "failed"
		base.ResultCode = "invalid_root"
		base.Summary = "The configured backup root is missing, inaccessible, or not a directory."
		base.FinishedAt = protocolTimestamp(now())
		return base, []byte(base.Summary + "\n"), false, 0
	}

	request := restic.Request{
		Version:   1,
		Operation: "backup",
		Connection: restic.Connection{
			Driver:    job.Repository.Connection.Driver,
			Path:      job.Repository.Connection.Path,
			Endpoint:  job.Repository.Connection.Endpoint,
			Bucket:    job.Repository.Connection.Bucket,
			Prefix:    job.Repository.Connection.Prefix,
			Region:    job.Repository.Connection.Region,
			AccessKey: job.Repository.Connection.AccessKey,
			SecretKey: job.Repository.Connection.SecretKey,
		},
		Password:        job.Repository.ServicePassword,
		Root:            job.Source.Root,
		Excludes:        append([]string(nil), job.Source.Excludes...),
		Host:            serverID,
		Tags:            []string{"backupchief-job:" + job.ID, "backupchief-run:" + command.RunID},
		TimeoutSeconds:  12 * 60 * 60,
		LockWaitSeconds: 5 * 60,
	}
	resticResult := executor.Run(ctx, request)
	base.FinishedAt = protocolTimestamp(now())
	log, truncated, dropped := retainedLog(resticResult)

	if resticResult.Outcome == "cancelled" {
		base.Status = "cancelled"
		base.ResultCode = "cancelled"
		base.Summary = "The backup was cancelled."
		if ctx.Err() == nil {
			base.Status = "failed"
			base.ResultCode = "timed_out"
			base.Summary = "The backup exceeded its execution deadline."
		}
		return base, log, truncated, dropped
	}

	summaries := parseResticSummaries(resticResult.Output)
	if (resticResult.ExitCode == 0 || resticResult.ExitCode == 3) && len(summaries) > 0 {
		summary := summaries[len(summaries)-1]
		statistics := &RunStatistics{
			"files_new":              summary.FilesNew,
			"files_changed":          summary.FilesChanged,
			"files_unmodified":       summary.FilesUnmodified,
			"directories_new":        summary.DirectoriesNew,
			"directories_changed":    summary.DirectoriesChanged,
			"directories_unmodified": summary.DirectoriesUnmodified,
			"source_files":           summary.SourceFiles,
			"source_bytes":           summary.SourceBytes,
			"stored_bytes":           summary.StoredBytes,
		}
		base.Statistics = statistics
		for _, item := range summaries {
			if digestPattern.MatchString(item.SnapshotID) && !contains(base.SnapshotIDs, item.SnapshotID) {
				base.SnapshotIDs = append(base.SnapshotIDs, item.SnapshotID)
			}
		}
		if len(base.SnapshotIDs) > 0 {
			if resticResult.ExitCode == 3 {
				base.Status = "partial"
				base.ResultCode = "unreadable_files"
				base.Summary = "The snapshot completed with unreadable files."
			} else if summary.SourceFiles == 0 {
				base.Status = "complete"
				base.ResultCode = "empty_selection"
				base.Summary = "The snapshot completed with no selected files."
			} else {
				base.Status = "complete"
				base.ResultCode = "success"
				base.Summary = "The snapshot completed successfully."
			}
			return base, log, truncated, dropped
		}
	}

	base.Status = "failed"
	base.ResultCode = resultCodeForExit(resticResult.ExitCode)
	base.Summary = "Restic did not produce a complete snapshot result."
	return base, log, truncated, dropped
}

func parseResticSummaries(output string) []resticSummary {
	var summaries []resticSummary
	scanner := bufio.NewScanner(strings.NewReader(output))
	scanner.Buffer(make([]byte, 64<<10), 2<<20)
	for scanner.Scan() {
		var summary resticSummary
		if json.Unmarshal(scanner.Bytes(), &summary) == nil && summary.MessageType == "summary" {
			summaries = append(summaries, summary)
		}
	}
	return summaries
}

func resultCodeForExit(exitCode int) string {
	switch exitCode {
	case 10:
		return "repository_missing"
	case 11:
		return "repository_locked"
	case 12:
		return "repository_authentication_failed"
	default:
		return "execution_failed"
	}
}

func retainedLog(result restic.Result) ([]byte, bool, uint64) {
	var log bytes.Buffer
	if result.Output != "" {
		log.WriteString(result.Output)
		if !strings.HasSuffix(result.Output, "\n") {
			log.WriteByte('\n')
		}
	}
	if result.Diagnostic != "" {
		log.WriteString(result.Diagnostic)
		if !strings.HasSuffix(result.Diagnostic, "\n") {
			log.WriteByte('\n')
		}
	}
	contents := log.Bytes()
	dropped := uint64(max(result.DroppedBytes, 0))
	truncated := result.Truncated
	if len(contents) > maximumRunLog {
		dropped += uint64(len(contents) - maximumRunLog)
		contents = append([]byte(nil), contents[:maximumRunLog]...)
		truncated = true
	} else {
		contents = append([]byte(nil), contents...)
	}
	return contents, truncated, dropped
}

func contains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func terminalEventPayload(result CommandResult) map[string]any {
	payload := map[string]any{
		"status":       result.Status,
		"result_code":  result.ResultCode,
		"started_at":   result.StartedAt,
		"finished_at":  result.FinishedAt,
		"snapshot_ids": result.SnapshotIDs,
	}
	if result.Statistics != nil {
		payload["statistics"] = result.Statistics
	}
	if result.Summary != "" {
		payload["summary"] = result.Summary
	}
	if result.RepositoryBytes != nil {
		payload["repository_bytes"] = *result.RepositoryBytes
	}
	return payload
}

func measureRepositoryBytes(ctx context.Context, executor BackupExecutor, job Job) *uint64 {
	request := maintenanceRequest(job)
	request.Operation = "stats"
	request.TimeoutSeconds = 5 * 60
	request.LockWaitSeconds = 30
	result := executor.Run(ctx, request)
	if result.ExitCode != 0 {
		return nil
	}

	var statistics resticRepositoryStats
	if json.Unmarshal([]byte(result.Output), &statistics) != nil {
		return nil
	}

	return &statistics.TotalSize
}

func describeExecution(result CommandResult) string {
	return fmt.Sprintf("%s/%s", result.Status, result.ResultCode)
}
