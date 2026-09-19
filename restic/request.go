package restic

import (
	"bytes"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/chieftools/backupchief-agent/egress"
)

const protocolVersion = 1

var bucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

var amazonS3EndpointPattern = regexp.MustCompile(`^s3[.-]([a-z0-9-]+)\.amazonaws\.com$`)

var snapshotPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

type Connection struct {
	Driver         string   `json:"driver"`
	Path           string   `json:"path,omitempty"`
	Endpoint       string   `json:"endpoint,omitempty"`
	Bucket         string   `json:"bucket,omitempty"`
	Prefix         string   `json:"prefix,omitempty"`
	Region         string   `json:"region,omitempty"`
	AccessKey      string   `json:"access_key,omitempty"`
	SecretKey      string   `json:"secret_key,omitempty"`
	Host           string   `json:"host,omitempty"`
	Port           uint16   `json:"port,omitempty"`
	Username       string   `json:"username,omitempty"`
	HostKeys       []string `json:"host_keys,omitempty"`
	SFTPPassword   string   `json:"password,omitempty"`
	SFTPPrivateKey string   `json:"private_key,omitempty"`
}

type transportOptions struct {
	rcloneConfig  string
	rcloneProgram string
	proxyURL      string
	obscure       func(string) (string, error)
}

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

func (r Request) dualArguments(passwordFile, sourcePasswordFile, cache string, transport transportOptions, local bool) ([]string, []string, string, error) {
	if r.Version != protocolVersion || r.SourceConnection == nil || r.Password == "" || r.SourcePassword == "" ||
		strings.ContainsAny(r.Password+r.SourcePassword, "\r\n\x00") ||
		r.TimeoutSeconds < 1 || r.TimeoutSeconds > 86400 || r.LockWaitSeconds < 0 || r.LockWaitSeconds > 300 || r.LockWaitSeconds >= r.TimeoutSeconds {
		return nil, nil, "", errors.New("invalid dual repository request")
	}

	lockWait := time.Duration(r.LockWaitSeconds) * time.Second
	arguments := []string{"--password-file", passwordFile, "--cache-dir", cache, "--retry-lock", lockWait.String()}
	environment := []string{"PATH=/usr/bin:/bin", "HOME=" + cache, "TMPDIR=" + cache, "LANG=C", "RESTIC_PROGRESS_FPS=1"}

	if r.Connection.Driver == "local" && r.SourceConnection.Driver == "local" {
		if err := validateLocalConnection(r.Connection, local); err != nil {
			return nil, nil, "", err
		}
		if err := validateLocalConnection(*r.SourceConnection, local); err != nil {
			return nil, nil, "", err
		}
		arguments = append(arguments, "--repo", r.Connection.Path)
		arguments = appendDualOperation(arguments, r.Operation, r.SourceConnection.Path, sourcePasswordFile)
		return arguments, environment, "", nil
	}
	if !filepath.IsAbs(transport.rcloneProgram) || strings.ContainsRune(transport.rcloneProgram, 0) {
		return nil, nil, "", errors.New("dual repository transport requires the bundled rclone executable")
	}

	sourceRepository, sourceSection, err := rcloneRepository("backupchief_source", *r.SourceConnection, local, transport)
	if err != nil {
		return nil, nil, "", err
	}
	destinationRepository, destinationSection, err := rcloneRepository("backupchief_destination", r.Connection, local, transport)
	if err != nil {
		return nil, nil, "", err
	}

	arguments = append(arguments, "--repo", destinationRepository, "-o", "rclone.program="+transport.rcloneProgram)
	arguments = appendDualOperation(arguments, r.Operation, sourceRepository, sourcePasswordFile)
	environment = append(environment, "RCLONE_CONFIG="+transport.rcloneConfig)

	return arguments, environment, sourceSection + destinationSection, nil
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

func validateLocalConnection(connection Connection, local bool) error {
	if !local || !filepath.IsAbs(connection.Path) || strings.ContainsRune(connection.Path, 0) {
		return errors.New("local repositories require an absolute path and local execution permission")
	}
	return nil
}

func rcloneRepository(name string, connection Connection, local bool, transport transportOptions) (string, string, error) {
	switch connection.Driver {
	case "local":
		if err := validateLocalConnection(connection, local); err != nil {
			return "", "", err
		}
		return "rclone:" + name + ":" + connection.Path, fmt.Sprintf("[%s]\ntype = local\n", name), nil
	case "s3":
		endpoint, err := canonicalS3Endpoint(connection)
		if err != nil || !bucketPattern.MatchString(connection.Bucket) || connection.Region == "" ||
			connection.AccessKey == "" || connection.SecretKey == "" || !safePrefix(connection.Prefix) ||
			strings.ContainsAny(connection.AccessKey+connection.SecretKey+connection.Region, "\r\n\x00") {
			return "", "", errors.New("invalid or missing S3 settings")
		}
		endpoint.Host = strings.TrimSuffix(endpoint.Host, ":443")
		section := fmt.Sprintf(
			"[%s]\ntype = s3\nprovider = Other\nenv_auth = false\naccess_key_id = %s\nsecret_access_key = %s\nendpoint = %s\nregion = %s\nforce_path_style = true\nno_check_bucket = true\n",
			name, connection.AccessKey, connection.SecretKey, endpoint.String(), connection.Region,
		)
		return "rclone:" + name + ":" + connection.Bucket + "/" + connection.Prefix, section, nil
	case "sftp":
		return sftpRcloneRepository(name, connection, transport, false)
	default:
		return "", "", errors.New("unsupported repository driver")
	}
}

func sftpRcloneRepository(name string, connection Connection, transport transportOptions, allowMissingHostKeys bool) (string, string, error) {
	if _, err := egress.Authority(egress.Target{Host: connection.Host, Port: connection.Port}); err != nil ||
		!safeSFTPValue(connection.Username, 255) || !safeSFTPPath(connection.Path) || (!allowMissingHostKeys && len(connection.HostKeys) == 0) || len(connection.HostKeys) > 16 ||
		(connection.SFTPPassword == "") == (connection.SFTPPrivateKey == "") {
		return "", "", errors.New("invalid or missing SFTP settings")
	}
	for _, hostKey := range connection.HostKeys {
		if !validSFTPHostKey(hostKey) {
			return "", "", errors.New("invalid or missing SFTP settings")
		}
	}

	authentication := ""
	if connection.SFTPPassword != "" {
		if !safeSFTPValue(connection.SFTPPassword, 1000) || transport.obscure == nil {
			return "", "", errors.New("invalid or missing SFTP settings")
		}
		password, err := transport.obscure(connection.SFTPPassword)
		if err != nil || !safeSFTPValue(password, 4096) {
			return "", "", errors.New("cannot prepare SFTP authentication")
		}
		authentication = "pass = " + password + "\n"
	} else {
		if !validEd25519PrivateKey(connection.SFTPPrivateKey) {
			return "", "", errors.New("invalid or missing SFTP settings")
		}
		authentication = "key_pem = " + strings.ReplaceAll(strings.TrimSpace(connection.SFTPPrivateKey), "\n", `\n`) + "\n"
	}

	section := fmt.Sprintf(
		"[%s]\ntype = sftp\nhost = %s\nport = %d\nuser = %s\n%sshell_type = none\ndisable_hashcheck = true\nhost_keys = %s\nhttp_proxy = %s\n",
		name,
		connection.Host,
		connection.Port,
		connection.Username,
		authentication,
		strings.Join(connection.HostKeys, ","),
		transport.proxyURL,
	)

	return "rclone:" + name + ":" + connection.Path, section, nil
}

func SFTPRemoteConfig(name string, connection Connection, obscuredPassword, proxyURL string, trustOnFirstUse bool) (string, string, error) {
	if name == "" || strings.ContainsAny(name, "[]\r\n\x00") {
		return "", "", errors.New("invalid SFTP remote name")
	}
	transport := transportOptions{
		proxyURL: proxyURL,
		obscure: func(string) (string, error) {
			return obscuredPassword, nil
		},
	}
	return sftpRcloneRepository(name, connection, transport, trustOnFirstUse)
}

func safeSFTPValue(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && !strings.ContainsAny(value, "\r\n\x00")
}

func safeSFTPPath(value string) bool {
	if value == "" || len(value) > 2048 || strings.ContainsAny(value, "\\\x00") || value == "~" || strings.HasPrefix(value, "~/") {
		return false
	}
	if value != "/" && (strings.HasSuffix(value, "/") || strings.Contains(value, "//")) {
		return false
	}
	if value == "/" {
		return true
	}
	for _, part := range strings.Split(strings.TrimPrefix(value, "/"), "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

func validSFTPHostKey(value string) bool {
	if !safeSFTPValue(value, 4096) {
		return false
	}
	parts := strings.Fields(value)
	if len(parts) != 2 || strings.Join(parts, " ") != value || !safeSFTPValue(parts[0], 255) {
		return false
	}
	decoded, err := base64.StdEncoding.DecodeString(parts[1])
	return err == nil && len(decoded) > 0 && len(decoded) <= 2048
}

func validEd25519PrivateKey(value string) bool {
	if value == "" || len(value) > 16384 || strings.ContainsRune(value, 0) {
		return false
	}
	block, trailing := pem.Decode([]byte(value))
	if block == nil || block.Type != "PRIVATE KEY" || len(bytes.TrimSpace(trailing)) != 0 {
		return false
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return false
	}
	_, ok := key.(ed25519.PrivateKey)
	return ok
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
	arguments, environment, _, err := r.argumentsWithTransport(passwordFile, newPasswordFile, cache, transportOptions{}, local)
	return arguments, environment, err
}

func (r Request) argumentsWithTransport(passwordFile, newPasswordFile, cache string, transport transportOptions, local bool) ([]string, []string, string, error) {
	if r.RecoverStaleLocks && !allowsStaleLockRecovery(r.Operation) {
		return nil, nil, "", errors.New("stale lock recovery is limited to maintenance operations")
	}

	arguments, environment, rcloneConfig, err := r.baseArgumentsWithTransport(passwordFile, cache, transport, local)
	if err != nil {
		return nil, nil, "", err
	}

	switch r.Operation {
	case "init":
		arguments = append(arguments, "init", "--repository-version", "2", "--json")
	case "key_add":
		if r.NewPassword == "" || strings.ContainsAny(r.NewPassword, "\r\n\x00") {
			return nil, nil, "", errors.New("invalid new password")
		}

		arguments = append(arguments, "key", "add", "--new-password-file", newPasswordFile)
	case "key_verify":
		arguments = append(arguments, "cat", "config")
	case "key_list":
		arguments = append(arguments, "key", "list", "--json")
	case "key_remove":
		if !snapshotPattern.MatchString(r.KeyID) {
			return nil, nil, "", errors.New("key removal requires a full key ID")
		}

		arguments = append(arguments, "key", "remove", r.KeyID)
	case "backup":
		if !filepath.IsAbs(r.Root) || strings.ContainsRune(r.Root, 0) {
			return nil, nil, "", errors.New("backup root must be absolute")
		}

		if strings.ContainsRune(r.Host, 0) || len(r.Host) > 255 {
			return nil, nil, "", errors.New("invalid backup host")
		}

		arguments = append(arguments, "backup", "--json", "--one-file-system")
		if r.Host != "" {
			arguments = append(arguments, "--host", r.Host)
		}
		arguments = append(arguments, "--exclude", cache)
		if r.Connection.Driver == "local" {
			arguments = append(arguments, "--exclude", r.Connection.Path)
		}
		for _, tag := range r.Tags {
			if tag == "" || strings.ContainsRune(tag, 0) || len(tag) > 255 {
				return nil, nil, "", errors.New("invalid backup tag")
			}
			arguments = append(arguments, "--tag", tag)
		}

		for _, exclude := range r.Excludes {
			if strings.ContainsRune(exclude, 0) {
				return nil, nil, "", errors.New("invalid exclude")
			}

			arguments = append(arguments, "--exclude", exclude)
		}

		arguments = append(arguments, "--", r.Root)
	case "backup_stdin":
		if !safeStdinFilename(r.StdinFilename) || len(r.StdinCommand) < 1 || len(r.StdinCommand) > 100 || !filepath.IsAbs(r.StdinCommand[0]) || r.CommandConfig == "" || len(r.CommandConfig) > 1<<20 || strings.ContainsRune(r.CommandConfig, 0) {
			return nil, nil, "", errors.New("stdin backup command is invalid")
		}
		for _, argument := range r.StdinCommand {
			if argument == "" || strings.ContainsRune(argument, 0) || len(argument) > 4096 {
				return nil, nil, "", errors.New("stdin backup command is invalid")
			}
		}
		if strings.ContainsRune(r.Host, 0) || len(r.Host) > 255 {
			return nil, nil, "", errors.New("invalid backup host")
		}
		arguments = append(arguments, "backup", "--json", "--stdin-from-command", "--stdin-filename", r.StdinFilename)
		if r.Host != "" {
			arguments = append(arguments, "--host", r.Host)
		}
		for _, tag := range r.Tags {
			if tag == "" || strings.ContainsRune(tag, 0) || len(tag) > 255 {
				return nil, nil, "", errors.New("invalid backup tag")
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
				return nil, nil, "", errors.New("invalid snapshot tag")
			}
			arguments = append(arguments, "--tag", tag)
		}
	case "ls":
		if !snapshotPattern.MatchString(r.Snapshot) || !safeSnapshotPath(r.Path) {
			return nil, nil, "", errors.New("directory listing requires a full snapshot ID and normalized absolute path")
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
			return nil, nil, "", errors.New("invalid data check subset")
		}
		arguments = append(arguments, "check", "--json", "--read-data-subset", strconv.Itoa(r.DataSubsetPart)+"/"+strconv.Itoa(r.DataSubsetTotal))
	case "forget_plan":
		if r.Retention == nil || retentionEmpty(*r.Retention) {
			return nil, nil, "", errors.New("forget plan requires a nonempty retention policy")
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
				return nil, nil, "", errors.New("invalid snapshot tag")
			}
			arguments = append(arguments, "--tag", tag)
		}
	case "forget":
		if len(r.SnapshotIDs) == 0 || len(r.SnapshotIDs) > 100 {
			return nil, nil, "", errors.New("forget requires one to 100 full snapshot IDs")
		}
		arguments = append(arguments, "forget")
		for _, snapshotID := range r.SnapshotIDs {
			if !snapshotPattern.MatchString(snapshotID) {
				return nil, nil, "", errors.New("forget requires one to 100 full snapshot IDs")
			}
			arguments = append(arguments, snapshotID)
		}
	case "prune":
		arguments = append(arguments, "prune")
	case "restore":
		if !snapshotPattern.MatchString(r.Snapshot) ||
			!filepath.IsAbs(r.Target) ||
			strings.ContainsRune(r.Target, 0) {
			return nil, nil, "", errors.New("restore requires a full snapshot ID and absolute target")
		}
		snapshot := r.Snapshot
		if r.Path != "" {
			if !safeSnapshotPath(r.Path) {
				return nil, nil, "", errors.New("restore path must be normalized and absolute")
			}
			snapshot += ":" + r.Path
		}

		arguments = append(arguments, "restore", snapshot, "--target", r.Target, "--verify", "--overwrite", "never")
	default:
		return nil, nil, "", errors.New("unsupported restic operation")
	}

	return arguments, environment, rcloneConfig, nil
}

func (r Request) staleLockArguments(passwordFile, cache string, local bool) ([]string, []string, error) {
	arguments, environment, _, err := r.staleLockArgumentsWithTransport(passwordFile, cache, transportOptions{}, local)
	return arguments, environment, err
}

func (r Request) staleLockArgumentsWithTransport(passwordFile, cache string, transport transportOptions, local bool) ([]string, []string, string, error) {
	r.LockWaitSeconds = 0
	r.RecoverStaleLocks = false
	arguments, environment, rcloneConfig, err := r.baseArgumentsWithTransport(passwordFile, cache, transport, local)
	if err != nil {
		return nil, nil, "", err
	}

	return append(arguments, "unlock"), environment, rcloneConfig, nil
}

func (r Request) baseArguments(passwordFile, cache string, local bool) ([]string, []string, error) {
	arguments, environment, _, err := r.baseArgumentsWithTransport(passwordFile, cache, transportOptions{}, local)
	return arguments, environment, err
}

func (r Request) baseArgumentsWithTransport(passwordFile, cache string, transport transportOptions, local bool) ([]string, []string, string, error) {
	if r.Version != protocolVersion || r.Password == "" || strings.ContainsAny(r.Password, "\r\n\x00") {
		return nil, nil, "", errors.New("invalid restic request or execution limits")
	}

	if r.TimeoutSeconds < 1 ||
		r.TimeoutSeconds > 86400 ||
		r.LockWaitSeconds < 0 ||
		r.LockWaitSeconds > 300 ||
		r.LockWaitSeconds >= r.TimeoutSeconds {
		return nil, nil, "", errors.New("invalid restic request or execution limits")
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

	connection := r.Connection

	switch connection.Driver {
	case "local":
		if !local || !filepath.IsAbs(connection.Path) || strings.ContainsRune(connection.Path, 0) {
			return nil, nil, "", errors.New("local repositories require an absolute path and local execution permission")
		}

		arguments = append(arguments, "--repo", connection.Path)
	case "s3":
		endpoint, err := canonicalS3Endpoint(connection)
		if err != nil {
			return nil, nil, "", err
		}

		if !bucketPattern.MatchString(connection.Bucket) ||
			connection.Region == "" ||
			connection.AccessKey == "" ||
			connection.SecretKey == "" ||
			!safePrefix(connection.Prefix) {
			return nil, nil, "", errors.New("invalid or missing S3 settings")
		}

		// Keep the canonical authority for TLS and request signing; the proxy pins only the dial address.
		endpoint.Host = strings.TrimSuffix(endpoint.Host, ":443")
		endpoint.Path = "/" + connection.Bucket + "/" + connection.Prefix

		arguments = append(
			arguments,
			"--repo",
			"s3:"+endpoint.String(),
			"-o",
			"s3.region="+connection.Region,
			"-o",
			"s3.bucket-lookup=path",
			"-o",
			"s3.retries=1",
		)
		environment = append(
			environment,
			"AWS_ACCESS_KEY_ID="+connection.AccessKey,
			"AWS_SECRET_ACCESS_KEY="+connection.SecretKey,
		)
	case "sftp":
		if !filepath.IsAbs(transport.rcloneProgram) || strings.ContainsRune(transport.rcloneProgram, 0) || transport.rcloneConfig == "" {
			return nil, nil, "", errors.New("SFTP requires the bundled rclone executable")
		}
		repository, section, err := rcloneRepository("backupchief_repository", connection, local, transport)
		if err != nil {
			return nil, nil, "", err
		}
		arguments = append(arguments, "--repo", repository, "-o", "rclone.program="+transport.rcloneProgram)
		environment = append(environment, "RCLONE_CONFIG="+transport.rcloneConfig)
		return arguments, environment, section, nil
	default:
		return nil, nil, "", errors.New("unsupported repository driver")
	}

	return arguments, environment, "", nil
}

func canonicalS3Endpoint(connection Connection) (*url.URL, error) {
	endpoint, err := egress.Endpoint(connection.Endpoint)
	if err != nil {
		return nil, err
	}

	matches := amazonS3EndpointPattern.FindStringSubmatch(endpoint.Hostname())
	if len(matches) == 2 && matches[1] == connection.Region {
		endpoint.Host = net.JoinHostPort("s3.dualstack."+connection.Region+".amazonaws.com", "443")
	}

	return endpoint, nil
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
