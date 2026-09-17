package updater

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	idPattern      = regexp.MustCompile(`^[0-9a-hjkmnp-tv-z]{26}$`)
	versionPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)
)

type Options struct {
	StateDirectory string
	ServiceUID     int
	ServiceGID     int
	PackageManager PackageManager
	ReadyTimeout   time.Duration
	PollEvery      time.Duration
	Now            func() time.Time
}

func Run(ctx context.Context, options Options) error {
	if options.StateDirectory == "" {
		return fmt.Errorf("updater state directory is required")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.ReadyTimeout == 0 {
		options.ReadyTimeout = 2 * time.Minute
	}
	if options.PollEvery == 0 {
		options.PollEvery = time.Second
	}

	request, err := readSecureRequest(options.StateDirectory, options.ServiceUID)
	if err != nil {
		return err
	}
	manager := options.PackageManager
	if manager == nil {
		manager, err = DetectPackageManager()
		if err != nil {
			return writeFailure(options, request, "preflight_failed", request.PreviousVersion, err)
		}
	}
	installed, err := manager.CurrentVersion(ctx)
	if err != nil {
		return writeFailure(options, request, "preflight_failed", request.PreviousVersion, err)
	}
	var persisted durableState
	if stateErr := readJSON(statePath(options.StateDirectory), &persisted); stateErr == nil {
		if !sameRequest(persisted.Request, request) {
			return fmt.Errorf("updater state does not match the active request")
		}
		request = persisted.Request
		switch persisted.Phase {
		case "installing":
			if installed == request.TargetVersion {
				if readyErr := waitForReadiness(ctx, options, request, request.TargetVersion); readyErr == nil {
					return writeTerminal(options, request, Result{Status: "complete", ResultCode: "success", InstalledVersion: request.TargetVersion})
				} else {
					return rollback(ctx, options, manager, request, persisted.PreviousPackageVersion, readyErr)
				}
			}
			if installed != persisted.PreviousPackageVersion {
				return rollback(ctx, options, manager, request, persisted.PreviousPackageVersion, fmt.Errorf("interrupted installation left unexpected package version %s", installed))
			}
		case "rolling_back":
			return resumeRollback(ctx, options, manager, persisted, installed)
		case "preflight":
		default:
			return fmt.Errorf("updater state phase is invalid")
		}
	} else if !errors.Is(stateErr, os.ErrNotExist) {
		return stateErr
	} else {
		request.PreviousVersion = installed
	}
	if installed == request.TargetVersion {
		return writeTerminal(options, request, Result{Status: "complete", ResultCode: "already_installed", InstalledVersion: installed})
	}
	if compareVersions(request.TargetVersion, installed) <= 0 {
		return writeFailure(options, request, "target_not_newer", installed, fmt.Errorf("target version is not newer than the installed package"))
	}

	state := durableState{Request: request, PreviousPackageVersion: installed, Phase: "preflight"}
	if err := writeJSON(statePath(options.StateDirectory), state, 0o600, options.ServiceUID, options.ServiceGID); err != nil {
		return err
	}
	if err := manager.Refresh(ctx); err != nil {
		return writeFailure(options, request, "preflight_failed", installed, err)
	}
	if err := manager.CheckAvailable(ctx, request.TargetVersion); err != nil {
		return writeFailure(options, request, "preflight_failed", installed, err)
	}
	if err := manager.CheckAvailable(ctx, installed); err != nil {
		return writeFailure(options, request, "preflight_failed", installed, fmt.Errorf("previous package is unavailable for rollback: %w", err))
	}

	_ = removeIfExists(ReadinessPath(options.StateDirectory))
	state.Phase = "installing"
	if err := writeJSON(statePath(options.StateDirectory), state, 0o600, options.ServiceUID, options.ServiceGID); err != nil {
		return err
	}
	if err := manager.Install(ctx, request.TargetVersion); err != nil {
		current, currentErr := manager.CurrentVersion(ctx)
		if currentErr == nil && current != installed {
			return rollback(ctx, options, manager, request, installed, fmt.Errorf("target installation failed: %w", err))
		}
		return writeFailure(options, request, "installation_failed", installed, err)
	}
	if err := waitForReadiness(ctx, options, request, request.TargetVersion); err == nil {
		return writeTerminal(options, request, Result{Status: "complete", ResultCode: "success", InstalledVersion: request.TargetVersion})
	} else {
		return rollback(ctx, options, manager, request, installed, err)
	}
}

func rollback(ctx context.Context, options Options, manager PackageManager, request Request, previous string, cause error) error {
	_ = removeIfExists(ReadinessPath(options.StateDirectory))
	state := durableState{Request: request, PreviousPackageVersion: previous, Phase: "rolling_back", Diagnostic: diagnostic(cause)}
	if err := writeJSON(statePath(options.StateDirectory), state, 0o600, options.ServiceUID, options.ServiceGID); err != nil {
		return err
	}
	if err := manager.Downgrade(ctx, previous); err != nil {
		return writeFailure(options, request, "rollback_failed", request.TargetVersion, errors.Join(cause, err))
	}
	if err := waitForReadiness(ctx, options, request, previous); err != nil {
		return writeFailure(options, request, "rollback_failed", previous, errors.Join(cause, err))
	}
	return writeTerminal(options, request, Result{
		Status: "failed", ResultCode: "readiness_failed_rolled_back", InstalledVersion: previous,
		Diagnostic: diagnostic(cause),
	})
}

func resumeRollback(ctx context.Context, options Options, manager PackageManager, state durableState, installed string) error {
	previous := state.PreviousPackageVersion
	if installed != previous {
		_ = removeIfExists(ReadinessPath(options.StateDirectory))
		if err := manager.Downgrade(ctx, previous); err != nil {
			return writeFailure(options, state.Request, "rollback_failed", installed, err)
		}
	}
	if err := waitForReadiness(ctx, options, state.Request, previous); err != nil {
		return writeFailure(options, state.Request, "rollback_failed", previous, err)
	}
	return writeTerminal(options, state.Request, Result{
		Status: "failed", ResultCode: "readiness_failed_rolled_back", InstalledVersion: previous,
		Diagnostic: state.Diagnostic,
	})
}

func waitForReadiness(ctx context.Context, options Options, request Request, version string) error {
	deadline := time.NewTimer(options.ReadyTimeout)
	ticker := time.NewTicker(options.PollEvery)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		var readiness Readiness
		if err := readJSON(ReadinessPath(options.StateDirectory), &readiness); err == nil &&
			readiness.CommandID == request.CommandID && readiness.RunID == request.RunID && readiness.Version == version {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("agent readiness timed out for version %s", version)
		case <-ticker.C:
		}
	}
}

func writeFailure(options Options, request Request, code, installed string, err error) error {
	return writeTerminal(options, request, Result{
		Status: "failed", ResultCode: code, InstalledVersion: installed, Diagnostic: diagnostic(err),
	})
}

func writeTerminal(options Options, request Request, result Result) error {
	result.Generation = request.Generation
	result.RunID = request.RunID
	result.PreviousVersion = request.PreviousVersion
	result.TargetVersion = request.TargetVersion
	result.StartedAt = request.StartedAt
	result.FinishedAt = protocolTimestamp(options.Now())
	if err := writeJSON(ResultPath(options.StateDirectory), result, 0o600, options.ServiceUID, options.ServiceGID); err != nil {
		return err
	}
	if err := removeIfExists(RequestPath(options.StateDirectory)); err != nil {
		return err
	}
	_ = removeIfExists(ReadinessPath(options.StateDirectory))
	_ = removeIfExists(statePath(options.StateDirectory))
	return nil
}

func statePath(directory string) string { return filepath.Join(directory, stateFile) }

func sameRequest(left, right Request) bool {
	return left.CommandID == right.CommandID && left.RunID == right.RunID && left.Generation == right.Generation &&
		left.TargetVersion == right.TargetVersion && left.StartedAt == right.StartedAt
}

func compareVersions(left, right string) int {
	leftParts := strings.Split(left, ".")
	rightParts := strings.Split(right, ".")
	for index := 0; index < 3; index++ {
		leftNumber, _ := strconv.Atoi(leftParts[index])
		rightNumber, _ := strconv.Atoi(rightParts[index])
		if leftNumber < rightNumber {
			return -1
		}
		if leftNumber > rightNumber {
			return 1
		}
	}
	return 0
}

func diagnostic(err error) string {
	if err == nil {
		return ""
	}
	value := err.Error()
	if len(value) > 4096 {
		value = value[:4096]
	}
	return value
}

func protocolTimestamp(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000000Z")
}
