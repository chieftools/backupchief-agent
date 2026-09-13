package agent

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/chieftools/backupchief-agent/restic"
)

func TestRealRepositoryMaintenanceAcrossSnapshotBoundaries(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	runner := restic.Runner{State: state, AllowLocal: true}
	repository := filepath.Join(t.TempDir(), "repository")
	password := "synthetic-maintenance-password"
	request := restic.Request{
		Version: 1, Operation: "init", Connection: restic.Connection{Driver: "local", Path: repository},
		Password: password, TimeoutSeconds: 300, LockWaitSeconds: 0,
	}
	requireMaintenanceComplete(t, runner.Run(context.Background(), request), "init")
	binary, err := restic.Binary(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}

	firstSource := filepath.Join(t.TempDir(), "source alpha")
	secondSource := filepath.Join(t.TempDir(), "source beta")
	for _, source := range []string{firstSource, secondSource} {
		if err := os.MkdirAll(source, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	history := []struct {
		at     string
		source string
		host   string
		tag    string
	}{
		{at: "2025-12-31 23:50:00", source: firstSource, host: "alpha.example.test", tag: "synthetic-alpha"},
		{at: "2026-01-01 00:10:00", source: secondSource, host: "beta.example.test", tag: "synthetic-beta"},
		{at: "2026-01-31 23:50:00", source: firstSource, host: "renamed.example.test", tag: "synthetic-renamed"},
		{at: "2026-02-01 00:10:00", source: secondSource, host: "beta.example.test", tag: "synthetic-beta"},
		{at: "2026-02-28 23:50:00", source: firstSource, host: "alpha.example.test", tag: "synthetic-alpha"},
		{at: "2026-03-01 00:10:00", source: secondSource, host: "renamed.example.test", tag: "synthetic-renamed"},
	}
	snapshotIDs := make([]string, 0, len(history))
	for index, item := range history {
		contents := []byte(strings.Repeat("synthetic maintenance data ", index+1))
		if err := os.WriteFile(filepath.Join(item.source, "archive.txt"), contents, 0o600); err != nil {
			t.Fatal(err)
		}
		snapshotIDs = append(snapshotIDs, seedMaintenanceSnapshot(t, binary, repository, password, item.source, item.at, item.host, item.tag))
	}

	job := Job{
		ID: "01k4p4f7m1r9d3t6v8w2x5y7za", Enabled: true,
		Repository: JobRepository{
			ID: strings.Repeat("a", 64), ServicePassword: password,
			Connection: RepositoryConnection{Driver: "local", Path: repository},
		},
		Retention: JobRetention{
			Last: 1, Daily: 2, Monthly: 2, KeepLatestComplete: true,
			LatestComplete: &CompleteSnapshotProof{
				RunID: "01k4p4f7m1r9d3t6v8w2x5y7zb", FinishedAt: "2025-12-31T23:51:00.000000Z", SnapshotIDs: []string{snapshotIDs[0]},
			},
		},
		Integrity: JobIntegrity{DataParts: 2},
	}
	var durablePlan MaintenancePlan
	result, _, _, _ := executeMaintenance(
		context.Background(), runner, 1, maintenanceJournalCommand("forget", job.ID), job, nil,
		func(plan MaintenancePlan) error { durablePlan = plan; return nil }, time.Now,
	)
	if result.Status != "complete" || result.ResultCode != "success" {
		t.Fatalf("forget: %+v", result)
	}
	if len(durablePlan.CandidateSnapshotIDs) != 3 {
		t.Fatalf("retention candidates: %+v", durablePlan)
	}

	request.Operation = "snapshots"
	remainingResult := runner.Run(context.Background(), request)
	requireMaintenanceComplete(t, remainingResult, "snapshots after forget")
	remaining := maintenanceSnapshotIDs(t, remainingResult.Output)
	wantRemaining := []string{snapshotIDs[0], snapshotIDs[4], snapshotIDs[5]}
	sort.Strings(wantRemaining)
	if !equalStrings(remaining, wantRemaining) {
		t.Fatalf("remaining snapshots: got %v want %v", remaining, wantRemaining)
	}

	for _, runKind := range []string{"prune", "check_metadata"} {
		maintenanceResult, _, _, _ := executeMaintenance(
			context.Background(), runner, 1, maintenanceJournalCommand(runKind, job.ID), job,
			nil, func(MaintenancePlan) error { return nil }, time.Now,
		)
		if maintenanceResult.ResultCode != "success" {
			t.Fatalf("%s: %+v", runKind, maintenanceResult)
		}
	}
	for part := uint64(1); part <= 2; part++ {
		check, _, _, _ := executeMaintenance(
			context.Background(), runner, 1, maintenanceJournalCommand("check_data", job.ID), job,
			&MaintenancePlan{Kind: "check_data", DataSubsetPart: part, DataSubsetTotal: 2}, func(MaintenancePlan) error { return nil }, time.Now,
		)
		if check.ResultCode != "success" || check.Statistics == nil || (*check.Statistics)["data_subset_part"] != part {
			t.Fatalf("data check part %d: %+v", part, check)
		}
	}

	pack := firstRepositoryPack(t, repository)
	if err := os.Remove(pack); err != nil {
		t.Fatal(err)
	}
	corruptVisible := false
	for part := uint64(1); part <= 2; part++ {
		check, _, _, _ := executeMaintenance(
			context.Background(), runner, 1, maintenanceJournalCommand("check_data", job.ID), job,
			&MaintenancePlan{Kind: "check_data", DataSubsetPart: part, DataSubsetTotal: 2}, func(MaintenancePlan) error { return nil }, time.Now,
		)
		corruptVisible = corruptVisible || check.ResultCode == "repository_corrupt"
	}
	if !corruptVisible {
		t.Fatal("missing repository data was not reported as corruption")
	}

	missingJob := job
	missingJob.Repository.Connection.Path = filepath.Join(t.TempDir(), "missing repository")
	missing, _, _, _ := executeMaintenance(
		context.Background(), runner, 1, maintenanceJournalCommand("prune", job.ID), missingJob,
		nil, func(MaintenancePlan) error { return nil }, time.Now,
	)
	if missing.ResultCode != "repository_missing" {
		t.Fatalf("missing repository: %+v", missing)
	}
}

func seedMaintenanceSnapshot(t *testing.T, binary, repository, password, source, at, host, tag string) string {
	t.Helper()
	command := exec.Command(
		binary, "--repo", repository, "backup", "--json", "--time", at,
		"--host", host, "--tag", tag, "--", source,
	)
	command.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir(), "RESTIC_PASSWORD=" + password, "LANG=C"}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("seed snapshot %s: %v %s", at, err, output)
	}
	var snapshotID string
	for _, line := range strings.Split(string(output), "\n") {
		var summary struct {
			MessageType string `json:"message_type"`
			SnapshotID  string `json:"snapshot_id"`
		}
		if json.Unmarshal([]byte(line), &summary) == nil && summary.MessageType == "summary" {
			snapshotID = summary.SnapshotID
		}
	}
	if !digestPattern.MatchString(snapshotID) {
		t.Fatalf("seed snapshot has no full ID: %s", output)
	}
	return snapshotID
}

func maintenanceSnapshotIDs(t *testing.T, output string) []string {
	t.Helper()
	var snapshots []struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(output), &snapshots); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		ids = append(ids, snapshot.ID)
	}
	sort.Strings(ids)
	return ids
}

func firstRepositoryPack(t *testing.T, repository string) string {
	t.Helper()
	var pack string
	err := filepath.WalkDir(filepath.Join(repository, "data"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && pack == "" {
			pack = path
		}
		return nil
	})
	if err != nil || pack == "" {
		t.Fatalf("repository pack: %q %v", pack, err)
	}
	return pack
}

func requireMaintenanceComplete(t *testing.T, result restic.Result, operation string) {
	t.Helper()
	if result.ExitCode != 0 || result.Outcome != "complete" {
		t.Fatalf("%s: %+v", operation, result)
	}
}

func equalStrings(first, second []string) bool {
	if len(first) != len(second) {
		return false
	}
	for index := range first {
		if first[index] != second[index] {
			return false
		}
	}
	return true
}
