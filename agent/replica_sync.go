package agent

import (
	"context"
	"time"

	"github.com/chieftools/backupchief-agent/restic"
)

func (daemon *daemon) startReplicaSync(ctx context.Context, commandID string, job Job) error {
	daemon.mu.Lock()
	journaled := daemon.journal.Commands[commandID]
	if journaled == nil || journaled.State != "received" {
		daemon.mu.Unlock()
		return nil
	}
	target, found := replicaForKey(job, journaled.Command.Payload.RepositoryKey)
	if !found {
		daemon.mu.Unlock()
		return daemon.finishWithoutExecution(commandID, "failed", "execution_failed", "The replica is unavailable in the required configuration.")
	}
	if daemon.replicationActive == nil {
		daemon.replicationActive = make(map[string]bool)
	}
	if daemon.repositories == nil {
		daemon.repositories = make(map[string]bool)
	}
	if daemon.replicationActive[job.ID] || daemon.repositories[job.Repository.ID] || daemon.repositories[target.ID] {
		daemon.mu.Unlock()
		return nil
	}

	daemon.replicationActive[job.ID] = true
	daemon.repositories[job.Repository.ID] = true
	daemon.repositories[target.ID] = true
	journaled.State = "running"
	if err := daemon.store.SaveCommandJournal(daemon.journal); err != nil {
		delete(daemon.replicationActive, job.ID)
		delete(daemon.repositories, job.Repository.ID)
		delete(daemon.repositories, target.ID)
		journaled.State = "received"
		daemon.mu.Unlock()
		return err
	}
	daemon.activeWG.Add(1)
	daemon.mu.Unlock()

	go daemon.runReplicaSync(ctx, commandID, job, target)
	return nil
}

func (daemon *daemon) runReplicaSync(ctx context.Context, commandID string, job Job, target JobRepository) {
	defer daemon.activeWG.Done()
	startedAt := daemon.now()
	delays := append([]time.Duration{0}, daemon.replicaSyncRetries...)
	var copyResult restic.Result

	for attempt, delay := range delays {
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				copyResult = restic.Result{Outcome: "cancelled", Diagnostic: ctx.Err().Error()}
				attempt = len(delays) - 1
			case <-timer.C:
			}
			if ctx.Err() != nil {
				break
			}
		}

		copyResult = daemon.executor.Run(ctx, restic.Request{
			Version: 1, Operation: "copy", Connection: replicaResticConnection(target.Connection),
			SourceConnection: connectionPointer(replicaResticConnection(job.Repository.Connection)),
			Password:         target.ServicePassword, SourcePassword: job.Repository.ServicePassword,
			TimeoutSeconds: 24 * 60 * 60, LockWaitSeconds: 5 * 60,
		})
		if copyResult.ExitCode == 0 || copyResult.ExitCode == 12 || attempt == len(delays)-1 {
			break
		}
	}

	finishedAt := daemon.now()
	status, resultCode := "complete", "success"
	summary := "Replica synchronization completed."
	diagnostic := ""
	var repositoryBytes *uint64
	if copyResult.ExitCode == 0 {
		_, repositoryBytes, diagnostic = daemon.replicaInventory(ctx, target)
	} else {
		status, resultCode = "failed", "copy_failed"
		summary = "Restic could not synchronize the replica."
		diagnostic = boundedReplicationDiagnostic(copyResult.Diagnostic)
		if copyResult.Outcome == "cancelled" {
			status, resultCode = "cancelled", "cancelled"
		}
	}

	daemon.mu.Lock()
	delete(daemon.replicationActive, job.ID)
	delete(daemon.repositories, job.Repository.ID)
	delete(daemon.repositories, target.ID)
	journaled := daemon.journal.Commands[commandID]
	if journaled != nil {
		journaled.State = "finished"
		journaled.Result = &CommandResult{
			Generation: daemon.bootstrap.Generation, RunID: journaled.RunID, JobID: job.ID,
			RepositoryKey: target.Key,
			Status:        status, ResultCode: resultCode, StartedAt: protocolTimestamp(startedAt),
			FinishedAt: protocolTimestamp(finishedAt), RepositoryBytes: repositoryBytes,
			Summary: summary, Diagnostic: diagnostic, SnapshotIDs: []string{},
		}
		journaled.Result.RepositoryResults = nil
		_ = daemon.store.SaveCommandJournal(daemon.journal)
	}
	daemon.mu.Unlock()
	notifyLoop(daemon.reportWake)
}

func replicaForKey(job Job, key string) (JobRepository, bool) {
	for _, repository := range append(append([]JobRepository{}, job.ReplicaSetups...), job.Replicas...) {
		if repository.Key == key {
			return repository, true
		}
	}
	return JobRepository{}, false
}
