package updater

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const (
	requestFile   = "updater-request.json"
	stateFile     = "updater-state.json"
	readinessFile = "updater-readiness.json"
	resultFile    = "updater-result.json"
	maximumFile   = 16 << 10
)

type Request struct {
	CommandID       string `json:"command_id"`
	RunID           string `json:"run_id"`
	Generation      uint64 `json:"generation"`
	PreviousVersion string `json:"previous_version"`
	TargetVersion   string `json:"target_version"`
	StartedAt       string `json:"started_at"`
}

type Readiness struct {
	CommandID string `json:"command_id"`
	RunID     string `json:"run_id"`
	Version   string `json:"version"`
}

type Result struct {
	Generation       uint64 `json:"generation"`
	RunID            string `json:"run_id"`
	Status           string `json:"status"`
	ResultCode       string `json:"result_code"`
	PreviousVersion  string `json:"previous_version"`
	TargetVersion    string `json:"target_version"`
	InstalledVersion string `json:"installed_version"`
	StartedAt        string `json:"started_at"`
	FinishedAt       string `json:"finished_at"`
	Diagnostic       string `json:"diagnostic,omitempty"`
}

type durableState struct {
	Request                Request `json:"request"`
	PreviousPackageVersion string  `json:"previous_package_version"`
	Phase                  string  `json:"phase"`
	Diagnostic             string  `json:"diagnostic,omitempty"`
}

func RequestPath(directory string) string   { return filepath.Join(directory, requestFile) }
func ResultPath(directory string) string    { return filepath.Join(directory, resultFile) }
func ReadinessPath(directory string) string { return filepath.Join(directory, readinessFile) }

func WriteRequest(directory string, request Request) error {
	if err := validateRequest(request); err != nil {
		return err
	}
	return writeJSON(RequestPath(directory), request, 0o600, -1, -1)
}

func WriteReadiness(directory string, readiness Readiness) error {
	if !idPattern.MatchString(readiness.CommandID) || !idPattern.MatchString(readiness.RunID) || !versionPattern.MatchString(readiness.Version) {
		return fmt.Errorf("updater readiness is invalid")
	}
	return writeJSON(ReadinessPath(directory), readiness, 0o600, -1, -1)
}

func ReadResult(directory string) (*Result, error) {
	var result Result
	if err := readJSON(ResultPath(directory), &result); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	return &result, nil
}

func RemoveResult(directory string) error {
	return removeIfExists(ResultPath(directory))
}

func readSecureRequest(directory string, expectedUID int) (Request, error) {
	path := RequestPath(directory)
	info, err := os.Lstat(path)
	if err != nil {
		return Request{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() > maximumFile {
		return Request{}, fmt.Errorf("updater request file is unsafe")
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); !ok || expectedUID >= 0 && int(stat.Uid) != expectedUID {
		return Request{}, fmt.Errorf("updater request owner is invalid")
	}
	var request Request
	if err := readJSON(path, &request); err != nil {
		return Request{}, err
	}
	if err := validateRequest(request); err != nil {
		return Request{}, err
	}
	return request, nil
}

func validateRequest(request Request) error {
	if !idPattern.MatchString(request.CommandID) || !idPattern.MatchString(request.RunID) || request.Generation == 0 ||
		!versionPattern.MatchString(request.PreviousVersion) || !versionPattern.MatchString(request.TargetVersion) || request.StartedAt == "" {
		return fmt.Errorf("updater request is invalid")
	}
	return nil
}

func readJSON(path string, target any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maximumFile+1))
	if err != nil {
		return err
	}
	if len(data) > maximumFile {
		return fmt.Errorf("%s exceeds its size limit", filepath.Base(path))
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode %s: %w", filepath.Base(path), err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("decode %s: trailing data", filepath.Base(path))
	}
	return nil
}

func writeJSON(path string, value any, mode os.FileMode, uid, gid int) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, "."+filepath.Base(path)+"-*")
	if err != nil {
		return err
	}
	temporary := file.Name()
	cleanup := func() {
		_ = file.Close()
		_ = os.Remove(temporary)
	}
	if err := file.Chmod(mode); err != nil {
		cleanup()
		return err
	}
	if uid >= 0 && gid >= 0 {
		if err := file.Chown(uid, gid); err != nil {
			cleanup()
			return err
		}
	}
	if _, err := file.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := file.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	return nil
}

func removeIfExists(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
