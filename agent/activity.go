package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"github.com/chieftools/backupchief-agent/restic"
)

type ActivitySnapshot struct {
	Kind          string            `json:"kind"`
	JobID         string            `json:"job_id"`
	RunID         string            `json:"run_id,omitempty"`
	CommandID     string            `json:"command_id,omitempty"`
	RepositoryKey string            `json:"repository_key,omitempty"`
	Attempt       string            `json:"attempt"`
	Sequence      uint64            `json:"sequence"`
	Stage         string            `json:"stage"`
	Counters      map[string]uint64 `json:"counters"`
}

type liveActivity struct {
	daemon *daemon
	keys   []string
}

func (daemon *daemon) beginActivity(kind string, commands []*JournalCommand, job Job, repositoryKey string) *liveActivity {
	activity := &liveActivity{daemon: daemon}
	daemon.mu.Lock()
	defer daemon.mu.Unlock()

	if daemon.activities == nil {
		daemon.activities = map[string]ActivitySnapshot{}
	}

	attempt, err := newULID(daemon.now())
	if err != nil {
		return activity
	}

	for _, command := range commands {
		snapshot := ActivitySnapshot{
			Kind:     kind,
			JobID:    prefixID(job.ID, "job_"),
			Attempt:  attempt,
			Stage:    "preparing",
			Counters: map[string]uint64{},
		}

		if kind == "sync" {
			snapshot.CommandID = command.Command.ID
		} else {
			snapshot.RunID = prefixID(command.RunID, "run_")
		}

		if kind != "run" {
			snapshot.RepositoryKey = repositoryKey
		}

		key := attempt + ":" + command.RunID
		daemon.activities[key] = snapshot
		activity.keys = append(activity.keys, key)
	}

	return activity
}

func (activity *liveActivity) update(stage string, counters map[string]uint64) {
	activity.daemon.mu.Lock()
	defer activity.daemon.mu.Unlock()

	for _, key := range activity.keys {
		snapshot, exists := activity.daemon.activities[key]
		if !exists {
			continue
		}

		if stage != "" {
			snapshot.Stage = stage
		}

		for name, value := range counters {
			snapshot.Counters[name] = min(value, 9007199254740991)
		}

		activity.daemon.activities[key] = snapshot
	}
}

func (activity *liveActivity) close() {
	activity.daemon.mu.Lock()
	defer activity.daemon.mu.Unlock()

	for _, key := range activity.keys {
		delete(activity.daemon.activities, key)
	}
}

func (daemon *daemon) reportActivity(ctx context.Context) error {
	if daemon.client == nil || !protocolRevisionSupports(daemon.client.selectedProtocolRevision(), "1.16.0") {
		return nil
	}

	snapshots := daemon.collectActivitySnapshots()

	for len(snapshots) > 0 {
		count := min(50, len(snapshots))
		if err := daemon.client.Activity(ctx, daemon.bootstrap.Generation, snapshots[:count]); err != nil {
			return err
		}

		snapshots = snapshots[count:]
	}

	return nil
}

func (daemon *daemon) collectActivitySnapshots() []ActivitySnapshot {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()

	keys := make([]string, 0, len(daemon.activities))
	for key := range daemon.activities {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	snapshots := make([]ActivitySnapshot, 0, len(keys))
	for _, key := range keys {
		snapshot := daemon.activities[key]
		snapshot.Sequence++
		daemon.activities[key] = snapshot

		counters := map[string]uint64{}
		for name, value := range snapshot.Counters {
			counters[name] = value
		}

		snapshot.Counters = counters
		snapshots = append(snapshots, snapshot)
	}

	return snapshots
}

func (client *Client) Activity(ctx context.Context, generation uint64, snapshots []ActivitySnapshot) error {
	body, err := json.Marshal(map[string]any{
		"generation": generation,
		"activities": snapshots,
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	response, err := client.request(ctx, http.MethodPost, "/activity", client.Credential, "", body)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if err := requireProtocolResponse(response); err != nil {
		return err
	}

	if response.StatusCode != http.StatusNoContent {
		return decodeProblem(response)
	}

	return nil
}

type activityExecutor struct {
	executor BackupExecutor
	activity *liveActivity
	bytes    uint64
	files    uint64
}

func (executor *activityExecutor) Run(ctx context.Context, request restic.Request) restic.Result {
	stage := map[string]string{
		"backup":         "backing_up",
		"backup_stdin":   "backing_up",
		"stats":          "measuring_repository",
		"snapshots":      "reading_snapshots",
		"forget_plan":    "planning_retention",
		"forget":         "removing_snapshots",
		"prune":          "pruning",
		"check_metadata": "checking_metadata",
		"check_data":     "checking_data",
		"copy":           "copying",
	}[request.Operation]
	if stage == "" {
		stage = "preparing"
	}
	executor.activity.update(stage, nil)

	var processed, files uint64
	observed := restic.WithProgress(ctx, func(progress restic.Progress) {
		currentStage := progress.Stage
		if currentStage == "executing" {
			currentStage = stage
		}

		counters := map[string]uint64{}
		for name, value := range progress.Counters {
			counters[name] = value
		}

		if request.Operation == "backup_stdin" {
			if value, exists := counters["bytes_processed"]; exists {
				processed = value
				counters["bytes_processed"] += executor.bytes
			}

			if value, exists := counters["files_processed"]; exists {
				files = value
				counters["files_processed"] += executor.files
			}
		}

		executor.activity.update(currentStage, counters)
	})

	result := executor.executor.Run(observed, request)
	if request.Operation == "backup_stdin" {
		for _, summary := range parseResticSummaries(result.Output) {
			processed = max(processed, summary.SourceBytes)
			files = max(files, summary.SourceFiles)
		}

		executor.bytes += processed
		executor.files += files
		executor.activity.update("", map[string]uint64{
			"bytes_processed": executor.bytes,
			"files_processed": executor.files,
		})
	}

	return result
}
