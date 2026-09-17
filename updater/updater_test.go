package updater

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

type fakeManager struct {
	current   string
	available map[string]bool
	install   func(string) error
	downgrade func(string) error
}

func (manager *fakeManager) Name() string { return "synthetic" }

func (manager *fakeManager) CurrentVersion(context.Context) (string, error) {
	return manager.current, nil
}

func (manager *fakeManager) Refresh(context.Context) error { return nil }

func (manager *fakeManager) CheckAvailable(_ context.Context, version string) error {
	if manager.available[version] {
		return nil
	}
	return errors.New("synthetic version unavailable")
}

func (manager *fakeManager) Install(_ context.Context, version string) error {
	return manager.install(version)
}

func (manager *fakeManager) Downgrade(_ context.Context, version string) error {
	return manager.downgrade(version)
}

func TestRunInstallsAndRecordsReadiness(t *testing.T) {
	directory := t.TempDir()
	request := testRequest()
	if err := WriteRequest(directory, request); err != nil {
		t.Fatal(err)
	}
	manager := &fakeManager{current: "2.4.0", available: map[string]bool{"2.4.0": true, "2.5.0": true}}
	manager.install = func(version string) error {
		return WriteReadiness(directory, Readiness{CommandID: request.CommandID, RunID: request.RunID, Version: version})
	}
	manager.downgrade = func(string) error { return nil }

	if err := Run(context.Background(), testOptions(directory, manager)); err != nil {
		t.Fatal(err)
	}
	result, err := ReadResult(directory)
	if err != nil || result == nil || result.ResultCode != "success" || result.InstalledVersion != "2.5.0" {
		t.Fatalf("unexpected result: %+v %v", result, err)
	}
}

func TestRunRollsBackWhenTheUpdatedAgentDoesNotBecomeReady(t *testing.T) {
	directory := t.TempDir()
	request := testRequest()
	if err := WriteRequest(directory, request); err != nil {
		t.Fatal(err)
	}
	manager := &fakeManager{current: "2.4.0", available: map[string]bool{"2.4.0": true, "2.5.0": true}}
	manager.install = func(string) error { return nil }
	manager.downgrade = func(version string) error {
		return WriteReadiness(directory, Readiness{CommandID: request.CommandID, RunID: request.RunID, Version: version})
	}

	if err := Run(context.Background(), testOptions(directory, manager)); err != nil {
		t.Fatal(err)
	}
	result, err := ReadResult(directory)
	if err != nil || result == nil || result.ResultCode != "readiness_failed_rolled_back" || result.InstalledVersion != "2.4.0" {
		t.Fatalf("unexpected result: %+v %v", result, err)
	}
}

func TestRunRejectsAnUnsafeRequestFile(t *testing.T) {
	directory := t.TempDir()
	if err := WriteRequest(directory, testRequest()); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(RequestPath(directory), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := Run(context.Background(), testOptions(directory, &fakeManager{})); err == nil {
		t.Fatal("unsafe request was accepted")
	}
}

func TestRunResumesReadinessAndRollbackAfterAnInterruptedInstall(t *testing.T) {
	directory := t.TempDir()
	request := testRequest()
	if err := WriteRequest(directory, request); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(statePath(directory), durableState{
		Request: request, PreviousPackageVersion: "2.4.0", Phase: "installing",
	}, 0o600, -1, -1); err != nil {
		t.Fatal(err)
	}
	manager := &fakeManager{current: "2.5.0", available: map[string]bool{"2.4.0": true, "2.5.0": true}}
	manager.install = func(string) error { t.Fatal("interrupted target was installed again"); return nil }
	manager.downgrade = func(version string) error {
		return WriteReadiness(directory, Readiness{CommandID: request.CommandID, RunID: request.RunID, Version: version})
	}

	if err := Run(context.Background(), testOptions(directory, manager)); err != nil {
		t.Fatal(err)
	}
	result, err := ReadResult(directory)
	if err != nil || result == nil || result.ResultCode != "readiness_failed_rolled_back" {
		t.Fatalf("unexpected resumed result: %+v %v", result, err)
	}
}

func testRequest() Request {
	return Request{
		CommandID: "01k4p4f7m1r9d3t6v8w2x5y7za", RunID: "01k4p4f7m1r9d3t6v8w2x5y7zb", Generation: 3,
		PreviousVersion: "2.4.0", TargetVersion: "2.5.0", StartedAt: "2026-09-17T10:00:00.000000Z",
	}
}

func testOptions(directory string, manager PackageManager) Options {
	return Options{
		StateDirectory: directory, ServiceUID: -1, ServiceGID: -1, PackageManager: manager,
		ReadyTimeout: 5 * time.Millisecond, PollEvery: time.Millisecond,
		Now: func() time.Time { return time.Date(2026, 9, 17, 10, 2, 0, 0, time.UTC) },
	}
}
