package agent

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/chieftools/backupchief-agent/restic"
)

type copiedSnapshot struct {
	ID       string `json:"id"`
	Original string `json:"original"`
}

func (daemon *daemon) resumeReplications(ctx context.Context) {
	daemon.mu.Lock()
	pendingJobs := make(map[string]bool)
	for _, command := range daemon.journal.Commands {
		if command.Result != nil && len(command.ReplicationPending) > 0 {
			pendingJobs[command.Result.JobID] = true
		}
	}
	jobs := append([]Job(nil), daemon.config.Jobs...)
	daemon.mu.Unlock()

	for _, job := range jobs {
		if pendingJobs[job.ID] {
			daemon.startReplication(ctx, job)
		}
	}
}

func (daemon *daemon) startReplication(ctx context.Context, job Job) {
	daemon.mu.Lock()
	if daemon.replicationActive == nil {
		daemon.replicationActive = make(map[string]bool)
	}
	if daemon.replicationActive[job.ID] {
		daemon.mu.Unlock()
		return
	}
	daemon.replicationActive[job.ID] = true
	daemon.activeWG.Add(1)
	daemon.mu.Unlock()

	go daemon.replicateJob(ctx, job)
}

func (daemon *daemon) replicateJob(ctx context.Context, job Job) {
	defer daemon.activeWG.Done()
	defer func() {
		daemon.mu.Lock()
		delete(daemon.replicationActive, job.ID)
		daemon.mu.Unlock()
		_ = daemon.startNextDeferredMaintenance(context.Background(), job.ID, false)
	}()

	for {
		target, commands := daemon.nextReplicationBatch(job)
		if target == nil || len(commands) == 0 {
			return
		}
		daemon.mu.Lock()
		if daemon.repositories == nil {
			daemon.repositories = make(map[string]bool)
		}
		if daemon.repositories[job.Repository.ID] || daemon.repositories[target.ID] {
			daemon.mu.Unlock()
			return
		}
		daemon.repositories[job.Repository.ID] = true
		daemon.repositories[target.ID] = true
		daemon.mu.Unlock()

		startedAt := daemon.now()
		copyResult := daemon.executor.Run(ctx, restic.Request{
			Version: 1, Operation: "copy", Connection: replicaResticConnection(target.Connection),
			SourceConnection: connectionPointer(replicaResticConnection(job.Repository.Connection)),
			Password:         target.ServicePassword, SourcePassword: job.Repository.ServicePassword,
			TimeoutSeconds: 24 * 60 * 60, LockWaitSeconds: 5 * 60,
		})
		finishedAt := daemon.now()
		daemon.mu.Lock()
		delete(daemon.repositories, job.Repository.ID)
		delete(daemon.repositories, target.ID)
		daemon.mu.Unlock()

		status, resultCode := "complete", "success"
		diagnostic := ""
		mappings := map[string]string{}
		var repositoryBytes *uint64
		if copyResult.ExitCode == 0 {
			mappings, repositoryBytes, diagnostic = daemon.replicaInventory(ctx, *target)
			for _, command := range commands {
				for _, snapshotID := range command.Result.SnapshotIDs {
					if mappings[snapshotID] == "" {
						status, resultCode = "failed", "copy_failed"
						if diagnostic == "" {
							diagnostic = "Replica inventory did not include every snapshot from this recovery point."
						}
					}
				}
			}
		} else {
			status, resultCode = "failed", "copy_failed"
			diagnostic = copyResult.Diagnostic
			if copyResult.Outcome == "cancelled" {
				resultCode = "cancelled"
				if ctx.Err() == nil {
					resultCode = "timed_out"
				}
			}
		}

		daemon.finishReplicationBatch(job, *target, commands, mappings, repositoryBytes, status, resultCode, diagnostic, startedAt, finishedAt)
		if status == "failed" {
			return
		}
	}
}

func (daemon *daemon) nextReplicationBatch(job Job) (*JobRepository, []*JournalCommand) {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()

	now := daemon.now()
	for index := range job.Replicas {
		replica := &job.Replicas[index]
		commands := make([]*JournalCommand, 0)
		for _, command := range daemon.journal.Commands {
			if command.State != "finished" || command.Result == nil || command.Result.JobID != job.ID || !slices.Contains(command.ReplicationPending, replica.Key) {
				continue
			}
			if retryAt := command.ReplicationRetryAt[replica.Key]; retryAt != "" {
				retryTime, err := time.Parse("2006-01-02T15:04:05.000000Z", retryAt)
				if err == nil && now.Before(retryTime) {
					continue
				}
			}
			commands = append(commands, command)
		}
		if len(commands) > 0 {
			return replica, commands
		}
	}
	return nil, nil
}

func (daemon *daemon) replicaInventory(ctx context.Context, repository JobRepository) (map[string]string, *uint64, string) {
	request := restic.Request{
		Version: 1, Operation: "snapshots", Connection: replicaResticConnection(repository.Connection), Password: repository.ServicePassword,
		TimeoutSeconds: 60 * 60, LockWaitSeconds: 5 * 60,
	}
	result := daemon.executor.Run(ctx, request)
	if result.ExitCode != 0 {
		return nil, nil, result.Diagnostic
	}
	var snapshots []copiedSnapshot
	if json.Unmarshal([]byte(result.Output), &snapshots) != nil {
		return nil, nil, "Replica inventory response was invalid."
	}
	mappings := make(map[string]string, len(snapshots)*2)
	for _, snapshot := range snapshots {
		if !digestPattern.MatchString(snapshot.ID) {
			continue
		}
		mappings[snapshot.ID] = snapshot.ID
		if digestPattern.MatchString(snapshot.Original) {
			mappings[snapshot.Original] = snapshot.ID
		}
	}

	request.Operation = "stats"
	stats := daemon.executor.Run(ctx, request)
	var parsed resticRepositoryStats
	if stats.ExitCode == 0 && json.Unmarshal([]byte(stats.Output), &parsed) == nil {
		return mappings, &parsed.TotalSize, ""
	}
	return mappings, nil, ""
}

func (daemon *daemon) finishReplicationBatch(job Job, repository JobRepository, commands []*JournalCommand, mappings map[string]string, repositoryBytes *uint64, status, resultCode, diagnostic string, startedAt, finishedAt time.Time) {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()

	for _, selected := range commands {
		command := daemon.journal.Commands[selected.Command.ID]
		if command == nil || command.Result == nil || !slices.Contains(command.ReplicationPending, repository.Key) {
			continue
		}
		snapshots := make([]map[string]any, 0, len(command.Result.SnapshotIDs))
		if status == "complete" {
			for _, original := range command.Result.SnapshotIDs {
				snapshots = append(snapshots, map[string]any{"original_snapshot_id": original, "snapshot_id": mappings[original]})
			}
		}
		payload := map[string]any{
			"repository_key": repository.Key, "repository_id": repository.ID, "status": status,
			"result_code": resultCode, "started_at": protocolTimestamp(startedAt), "finished_at": protocolTimestamp(finishedAt),
			"snapshots": snapshots,
		}
		if repositoryBytes != nil {
			payload["repository_bytes"] = *repositoryBytes
		}
		if status == "failed" {
			payload["diagnostic"] = boundedReplicationDiagnostic(diagnostic)
		}
		command.Sequence++
		if eventID, err := newULID(daemon.now()); err == nil {
			command.Events = append(command.Events, daemon.eventEnvelope(command, eventID, command.Sequence, protocolTimestamp(finishedAt), "replication_finished", payload))
		}
		if status == "complete" {
			command.ReplicationPending = removeString(command.ReplicationPending, repository.Key)
			delete(command.ReplicationRetryAt, repository.Key)
		} else {
			if command.ReplicationRetryAt == nil {
				command.ReplicationRetryAt = make(map[string]string)
			}
			command.ReplicationRetryAt[repository.Key] = protocolTimestamp(daemon.now().Add(15 * time.Minute))
		}
	}
	_ = daemon.store.SaveCommandJournal(daemon.journal)
	notifyLoop(daemon.reportWake)
}

func boundedReplicationDiagnostic(diagnostic string) string {
	diagnostic = strings.TrimSpace(diagnostic)
	if diagnostic == "" {
		return "Replica copy did not complete."
	}

	runes := []rune(diagnostic)
	if len(runes) > 4096 {
		diagnostic = string(runes[:4096])
	}

	return diagnostic
}

func replicaResticConnection(connection RepositoryConnection) restic.Connection {
	return restic.Connection{
		Driver: connection.Driver, Path: connection.Path, Endpoint: connection.Endpoint, Bucket: connection.Bucket,
		Prefix: connection.Prefix, Region: connection.Region, AccessKey: connection.AccessKey, SecretKey: connection.SecretKey,
	}
}

func connectionPointer(connection restic.Connection) *restic.Connection {
	return &connection
}

func removeString(values []string, target string) []string {
	filtered := values[:0]
	for _, value := range values {
		if value != target {
			filtered = append(filtered, value)
		}
	}
	sort.Strings(filtered)
	return filtered
}
