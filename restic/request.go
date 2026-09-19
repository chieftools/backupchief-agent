package restic

import (
	"errors"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/chieftools/backupchief-agent/repository"
)

const protocolVersion = 1

var snapshotPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

type Connection = repository.Connection

type Request struct {
	Version           int         `json:"version"`
	Operation         string      `json:"operation"`
	Connection        Connection  `json:"connection"`
	SourceConnection  *Connection `json:"source_connection,omitempty"`
	Password          string      `json:"password"`
	SourcePassword    string      `json:"source_password,omitempty"`
	NewPassword       string      `json:"new_password,omitempty"`
	KeyID             string      `json:"key_id,omitempty"`
	Root              string      `json:"root,omitempty"`
	Excludes          []string    `json:"excludes,omitempty"`
	Host              string      `json:"host,omitempty"`
	Tags              []string    `json:"tags,omitempty"`
	Snapshot          string      `json:"snapshot,omitempty"`
	SnapshotIDs       []string    `json:"snapshot_ids,omitempty"`
	Retention         *Retention  `json:"retention,omitempty"`
	DataSubsetPart    int         `json:"data_subset_part,omitempty"`
	DataSubsetTotal   int         `json:"data_subset_total,omitempty"`
	Target            string      `json:"target,omitempty"`
	Path              string      `json:"path,omitempty"`
	StdinFilename     string      `json:"stdin_filename,omitempty"`
	StdinCommand      []string    `json:"stdin_command,omitempty"`
	CommandConfig     string      `json:"command_config,omitempty"`
	TimeoutSeconds    int         `json:"timeout_seconds"`
	LockWaitSeconds   int         `json:"lock_wait_seconds"`
	RecoverStaleLocks bool        `json:"recover_stale_locks,omitempty"`
}

func (r Request) dualArguments(passwordFile, sourcePasswordFile, cache string, destination, source repository.PreparedRepository) ([]string, []string, error) {
	if r.Version != protocolVersion || r.SourceConnection == nil || r.Password == "" || r.SourcePassword == "" ||
		strings.ContainsAny(r.Password+r.SourcePassword, "\r\n\x00") ||
		r.TimeoutSeconds < 1 || r.TimeoutSeconds > 86400 || r.LockWaitSeconds < 0 || r.LockWaitSeconds > 300 || r.LockWaitSeconds >= r.TimeoutSeconds {
		return nil, nil, errors.New("invalid dual repository request")
	}

	lockWait := time.Duration(r.LockWaitSeconds) * time.Second
	arguments := []string{"--password-file", passwordFile, "--cache-dir", cache, "--retry-lock", lockWait.String()}
	environment := []string{"PATH=/usr/bin:/bin", "HOME=" + cache, "TMPDIR=" + cache, "LANG=C", "RESTIC_PROGRESS_FPS=1"}
	arguments = append(arguments, "--repo", destination.Repository)
	options := append([]string(nil), destination.Options...)
	for _, option := range source.Options {
		if !slices.Contains(options, option) {
			options = append(options, option)
		}
	}
	arguments = appendRepositoryOptions(arguments, options)
	arguments = appendDualOperation(arguments, r.Operation, source.Repository, sourcePasswordFile)
	return arguments, environment, nil
}

func appendDualOperation(arguments []string, operation, sourceRepository, sourcePasswordFile string) []string {
	switch operation {
	case "init_from":
		return append(arguments, "init", "--repository-version", "2", "--copy-chunker-params", "--from-repo", sourceRepository, "--from-password-file", sourcePasswordFile, "--json")
	case "copy":
		return append(arguments, "copy", "--from-repo", sourceRepository, "--from-password-file", sourcePasswordFile, "--json")
	default:
		return append(arguments, "unsupported-dual-operation")
	}
}

type Retention struct {
	Last    uint64 `json:"last"`
	Hourly  uint64 `json:"hourly"`
	Daily   uint64 `json:"daily"`
	Weekly  uint64 `json:"weekly"`
	Monthly uint64 `json:"monthly"`
	Yearly  uint64 `json:"yearly"`
}

type Result struct {
	Version      int    `json:"version"`
	ExitCode     int    `json:"exit_code"`
	Outcome      string `json:"outcome"`
	Output       string `json:"output"`
	Diagnostic   string `json:"diagnostic"`
	Truncated    bool   `json:"truncated"`
	DroppedBytes int    `json:"dropped_bytes,omitempty"`
}

func (r Request) arguments(passwordFile, newPasswordFile, cache string, local bool) ([]string, []string, error) {
	prepared, extraEnvironment, err := repository.PrepareNative(r.Connection, local)
	if err != nil {
		return nil, nil, err
	}
	arguments, environment, err := r.argumentsPrepared(passwordFile, newPasswordFile, cache, prepared)
	return arguments, append(environment, extraEnvironment...), err
}

func (r Request) argumentsPrepared(passwordFile, newPasswordFile, cache string, prepared repository.PreparedRepository) ([]string, []string, error) {
	if r.RecoverStaleLocks && !allowsStaleLockRecovery(r.Operation) {
		return nil, nil, errors.New("stale lock recovery is limited to maintenance operations")
	}

	arguments, environment, err := r.baseArgumentsPrepared(passwordFile, cache, prepared)
	if err != nil {
		return nil, nil, err
	}

	switch r.Operation {
	case "init":
		arguments = append(arguments, "init", "--repository-version", "2", "--json")
	case "key_add":
		if r.NewPassword == "" || strings.ContainsAny(r.NewPassword, "\r\n\x00") {
			return nil, nil, errors.New("invalid new password")
		}

		arguments = append(arguments, "key", "add", "--new-password-file", newPasswordFile)
	case "key_verify":
		arguments = append(arguments, "cat", "config")
	case "key_list":
		arguments = append(arguments, "key", "list", "--json")
	case "key_remove":
		if !snapshotPattern.MatchString(r.KeyID) {
			return nil, nil, errors.New("key removal requires a full key ID")
		}

		arguments = append(arguments, "key", "remove", r.KeyID)
	case "backup":
		if !filepath.IsAbs(r.Root) || strings.ContainsRune(r.Root, 0) {
			return nil, nil, errors.New("backup root must be absolute")
		}

		if strings.ContainsRune(r.Host, 0) || len(r.Host) > 255 {
			return nil, nil, errors.New("invalid backup host")
		}

		arguments = append(arguments, "backup", "--json", "--one-file-system")
		if r.Host != "" {
			arguments = append(arguments, "--host", r.Host)
		}
		arguments = append(arguments, "--exclude", cache)
		if local, ok := r.Connection.Local(); ok {
			arguments = append(arguments, "--exclude", local.Path)
		}
		for _, tag := range r.Tags {
			if tag == "" || strings.ContainsRune(tag, 0) || len(tag) > 255 {
				return nil, nil, errors.New("invalid backup tag")
			}
			arguments = append(arguments, "--tag", tag)
		}

		for _, exclude := range r.Excludes {
			if strings.ContainsRune(exclude, 0) {
				return nil, nil, errors.New("invalid exclude")
			}

			arguments = append(arguments, "--exclude", exclude)
		}

		arguments = append(arguments, "--", r.Root)
	case "backup_stdin":
		if !safeStdinFilename(r.StdinFilename) || len(r.StdinCommand) < 1 || len(r.StdinCommand) > 100 || !filepath.IsAbs(r.StdinCommand[0]) || r.CommandConfig == "" || len(r.CommandConfig) > 1<<20 || strings.ContainsRune(r.CommandConfig, 0) {
			return nil, nil, errors.New("stdin backup command is invalid")
		}
		for _, argument := range r.StdinCommand {
			if argument == "" || strings.ContainsRune(argument, 0) || len(argument) > 4096 {
				return nil, nil, errors.New("stdin backup command is invalid")
			}
		}
		if strings.ContainsRune(r.Host, 0) || len(r.Host) > 255 {
			return nil, nil, errors.New("invalid backup host")
		}
		arguments = append(arguments, "backup", "--json", "--stdin-from-command", "--stdin-filename", r.StdinFilename)
		if r.Host != "" {
			arguments = append(arguments, "--host", r.Host)
		}
		for _, tag := range r.Tags {
			if tag == "" || strings.ContainsRune(tag, 0) || len(tag) > 255 {
				return nil, nil, errors.New("invalid backup tag")
			}
			arguments = append(arguments, "--tag", tag)
		}
		arguments = append(arguments, "--", r.StdinCommand[0])
		arguments = append(arguments, r.StdinCommand[1:]...)
	case "snapshots":
		arguments = append(arguments, "snapshots", "--json")
		if r.Host != "" {
			arguments = append(arguments, "--host", r.Host)
		}
		for _, tag := range r.Tags {
			if tag == "" || strings.ContainsRune(tag, 0) || len(tag) > 255 {
				return nil, nil, errors.New("invalid snapshot tag")
			}
			arguments = append(arguments, "--tag", tag)
		}
	case "ls":
		if !snapshotPattern.MatchString(r.Snapshot) || !safeSnapshotPath(r.Path) {
			return nil, nil, errors.New("directory listing requires a full snapshot ID and normalized absolute path")
		}

		arguments = append(arguments, "cat", "tree", r.Snapshot+":"+r.Path)
	case "stats":
		arguments = append(arguments, "stats", "--mode", "raw-data", "--json")
	case "check":
		arguments = append(arguments, "check", "--read-data")
	case "check_metadata":
		arguments = append(arguments, "check", "--json")
	case "check_data":
		if r.DataSubsetTotal < 2 || r.DataSubsetTotal > 12 || r.DataSubsetPart < 1 || r.DataSubsetPart > r.DataSubsetTotal {
			return nil, nil, errors.New("invalid data check subset")
		}
		arguments = append(arguments, "check", "--json", "--read-data-subset", strconv.Itoa(r.DataSubsetPart)+"/"+strconv.Itoa(r.DataSubsetTotal))
	case "forget_plan":
		if r.Retention == nil || retentionEmpty(*r.Retention) {
			return nil, nil, errors.New("forget plan requires a nonempty retention policy")
		}
		arguments = append(arguments,
			"forget", "--dry-run", "--json", "--group-by", "",
			"--keep-last", strconv.FormatUint(r.Retention.Last, 10),
			"--keep-hourly", strconv.FormatUint(r.Retention.Hourly, 10),
			"--keep-daily", strconv.FormatUint(r.Retention.Daily, 10),
			"--keep-weekly", strconv.FormatUint(r.Retention.Weekly, 10),
			"--keep-monthly", strconv.FormatUint(r.Retention.Monthly, 10),
			"--keep-yearly", strconv.FormatUint(r.Retention.Yearly, 10),
		)
		for _, tag := range r.Tags {
			if tag == "" || strings.ContainsRune(tag, 0) || len(tag) > 255 {
				return nil, nil, errors.New("invalid snapshot tag")
			}
			arguments = append(arguments, "--tag", tag)
		}
	case "forget":
		if len(r.SnapshotIDs) == 0 || len(r.SnapshotIDs) > 100 {
			return nil, nil, errors.New("forget requires one to 100 full snapshot IDs")
		}
		arguments = append(arguments, "forget")
		for _, snapshotID := range r.SnapshotIDs {
			if !snapshotPattern.MatchString(snapshotID) {
				return nil, nil, errors.New("forget requires one to 100 full snapshot IDs")
			}
			arguments = append(arguments, snapshotID)
		}
	case "prune":
		arguments = append(arguments, "prune")
	case "restore":
		if !snapshotPattern.MatchString(r.Snapshot) ||
			!filepath.IsAbs(r.Target) ||
			strings.ContainsRune(r.Target, 0) {
			return nil, nil, errors.New("restore requires a full snapshot ID and absolute target")
		}
		snapshot := r.Snapshot
		if r.Path != "" {
			if !safeSnapshotPath(r.Path) {
				return nil, nil, errors.New("restore path must be normalized and absolute")
			}
			snapshot += ":" + r.Path
		}

		arguments = append(arguments, "restore", snapshot, "--target", r.Target, "--verify", "--overwrite", "never")
	default:
		return nil, nil, errors.New("unsupported restic operation")
	}

	return arguments, environment, nil
}

func (r Request) staleLockArguments(passwordFile, cache string, local bool) ([]string, []string, error) {
	prepared, extraEnvironment, err := repository.PrepareNative(r.Connection, local)
	if err != nil {
		return nil, nil, err
	}
	arguments, environment, err := r.staleLockArgumentsPrepared(passwordFile, cache, prepared)
	return arguments, append(environment, extraEnvironment...), err
}

func (r Request) staleLockArgumentsPrepared(passwordFile, cache string, prepared repository.PreparedRepository) ([]string, []string, error) {
	r.LockWaitSeconds = 0
	r.RecoverStaleLocks = false
	arguments, environment, err := r.baseArgumentsPrepared(passwordFile, cache, prepared)
	if err != nil {
		return nil, nil, err
	}

	return append(arguments, "unlock"), environment, nil
}

func (r Request) baseArguments(passwordFile, cache string, local bool) ([]string, []string, error) {
	prepared, extraEnvironment, err := repository.PrepareNative(r.Connection, local)
	if err != nil {
		return nil, nil, err
	}
	arguments, environment, err := r.baseArgumentsPrepared(passwordFile, cache, prepared)
	return arguments, append(environment, extraEnvironment...), err
}

func (r Request) baseArgumentsPrepared(passwordFile, cache string, prepared repository.PreparedRepository) ([]string, []string, error) {
	if r.Version != protocolVersion || r.Password == "" || strings.ContainsAny(r.Password, "\r\n\x00") {
		return nil, nil, errors.New("invalid restic request or execution limits")
	}

	if r.TimeoutSeconds < 1 ||
		r.TimeoutSeconds > 86400 ||
		r.LockWaitSeconds < 0 ||
		r.LockWaitSeconds > 300 ||
		r.LockWaitSeconds >= r.TimeoutSeconds {
		return nil, nil, errors.New("invalid restic request or execution limits")
	}

	lockWait := time.Duration(r.LockWaitSeconds) * time.Second
	arguments := []string{
		"--password-file",
		passwordFile,
		"--cache-dir",
		cache,
		"--retry-lock",
		lockWait.String(),
	}
	environment := []string{
		"PATH=/usr/bin:/bin",
		"HOME=" + cache,
		"TMPDIR=" + cache,
		"LANG=C",
		"RESTIC_PROGRESS_FPS=1",
	}

	arguments = append(arguments, "--repo", prepared.Repository)
	arguments = appendRepositoryOptions(arguments, prepared.Options)
	return arguments, environment, nil
}

func appendRepositoryOptions(arguments, options []string) []string {
	for _, option := range options {
		arguments = append(arguments, "-o", option)
	}
	return arguments
}

func allowsStaleLockRecovery(operation string) bool {
	switch operation {
	case "snapshots", "forget_plan", "forget", "prune", "check_metadata", "check_data":
		return true
	default:
		return false
	}
}

func safeStdinFilename(value string) bool {
	return value != "" && len(value) <= 255 && filepath.Base(value) == value && value != "." && value != ".." && !strings.ContainsAny(value, "/\\\x00\r\n")
}

func safeSnapshotPath(value string) bool {
	if value == "" || !strings.HasPrefix(value, "/") || strings.ContainsAny(value, "\\\x00") {
		return false
	}

	if value != "/" && (strings.HasSuffix(value, "/") || strings.Contains(value, "//")) {
		return false
	}

	for _, part := range strings.Split(value, "/") {
		if part == "." || part == ".." {
			return false
		}
	}

	return true
}

func retentionEmpty(retention Retention) bool {
	return retention.Last == 0 && retention.Hourly == 0 && retention.Daily == 0 && retention.Weekly == 0 && retention.Monthly == 0 && retention.Yearly == 0
}

func safePrefix(prefix string) bool {
	if prefix == "" || strings.ContainsAny(prefix, "\\\x00") {
		return false
	}
	for _, part := range strings.Split(prefix, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}

	return true
}
