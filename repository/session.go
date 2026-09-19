package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/chieftools/backupchief-agent/egress"
	bundledrclone "github.com/chieftools/backupchief-agent/rclone"
)

type Strategy uint8

const (
	PreferNative Strategy = iota
	RequireRclone
)

type Binding struct {
	Name            string
	Connection      Connection
	Strategy        Strategy
	TrustOnFirstUse bool
}

type SessionOptions struct {
	State      string
	Work       string
	AllowLocal bool
}

type PreparedRepository struct {
	Repository string
	Options    []string
}

type Session struct {
	work        string
	program     string
	configPath  string
	environment []string
	prepared    map[string]PreparedRepository
	proxy       *egress.Proxy
	obscure     func(string) (string, error)
	closeOnce   sync.Once
}

func OpenSession(ctx context.Context, options SessionOptions, bindings []Binding) (*Session, error) {
	if !filepath.IsAbs(options.State) || !filepath.IsAbs(options.Work) || len(bindings) == 0 {
		return nil, errors.New("invalid repository session")
	}

	session := &Session{work: options.Work, prepared: make(map[string]PreparedRepository, len(bindings))}
	names := make(map[string]struct{}, len(bindings))
	targets := make([]egress.Target, 0, len(bindings))
	requiresRclone := false
	for _, binding := range bindings {
		if !validRemoteName(binding.Name) {
			return nil, errors.New("invalid repository binding name")
		}
		if _, exists := names[binding.Name]; exists {
			return nil, errors.New("duplicate repository binding name")
		}
		names[binding.Name] = struct{}{}
		if err := validateBinding(binding, options.AllowLocal); err != nil {
			return nil, err
		}
		target, remote, err := binding.Connection.Target()
		if err != nil {
			return nil, err
		}
		if remote {
			targets = append(targets, target)
		}
		if binding.Strategy == RequireRclone || !supportsNative(binding.Connection) {
			requiresRclone = true
		}
	}

	if len(targets) > 0 {
		proxy, err := egress.StartTargets(ctx, targets)
		if err != nil {
			return nil, errors.New("cannot establish guarded storage transport")
		}
		session.proxy = proxy
		session.environment = proxyEnvironment(proxy.URL)
	}

	cleanupError := func(err error) (*Session, error) {
		session.Close()
		return nil, err
	}

	obscured := make(map[string]string)
	obscure := func(password string) (string, error) {
		if value, exists := obscured[password]; exists {
			return value, nil
		}
		command := exec.CommandContext(ctx, session.program, "obscure", "-")
		command.Env = commandEnvironment(options.Work, session.environment)
		command.Dir = options.Work
		command.Stdin = strings.NewReader(password)
		output, err := command.Output()
		if err != nil {
			return "", err
		}
		value := strings.TrimSpace(string(output))
		obscured[password] = value
		return value, nil
	}
	session.obscure = obscure

	if requiresRclone {
		program, err := bundledrclone.Binary(ctx, options.State)
		if err != nil {
			return cleanupError(errors.New("cannot prepare bundled rclone executable"))
		}
		session.program = program
		session.configPath = filepath.Join(options.Work, "rclone.conf")
	}

	var configuration strings.Builder
	for _, binding := range bindings {
		useRclone := binding.Strategy == RequireRclone || !supportsNative(binding.Connection)
		var prepared PreparedRepository
		var section string
		var environment []string
		var err error
		if useRclone {
			prepared, section, err = rcloneSpec(binding, session.program, session.proxyURL(), obscure)
		} else {
			prepared, environment, err = nativeSpec(binding.Connection)
		}
		if err != nil {
			return cleanupError(err)
		}
		configuration.WriteString(section)
		session.environment = appendUnique(session.environment, environment...)
		session.prepared[binding.Name] = prepared
	}

	if configuration.Len() > 0 {
		if err := os.WriteFile(session.configPath, []byte(configuration.String()), 0o600); err != nil {
			return cleanupError(errors.New("cannot write private transport configuration"))
		}
		session.environment = appendUnique(session.environment, "RCLONE_CONFIG="+session.configPath)
	}

	return session, nil
}

func (session *Session) Repository(name string) (PreparedRepository, bool) {
	prepared, ok := session.prepared[name]
	prepared.Options = append([]string(nil), prepared.Options...)
	return prepared, ok
}

func (session *Session) Environment() []string {
	return append([]string(nil), session.environment...)
}

func (session *Session) RcloneConfigPath() string { return session.configPath }

func (session *Session) RcloneCommand(ctx context.Context, arguments ...string) (*exec.Cmd, error) {
	if session.program == "" || session.configPath == "" {
		return nil, errors.New("repository session does not use rclone")
	}
	base := []string{"--config", session.configPath}
	command := exec.CommandContext(ctx, session.program, append(base, arguments...)...)
	command.Env = commandEnvironment(session.work, session.environment)
	command.Dir = session.work
	return command, nil
}

func (session *Session) RewriteRclone(binding Binding, allowLocal bool) error {
	if session.program == "" || session.configPath == "" || session.obscure == nil {
		return errors.New("repository session does not use rclone")
	}
	if err := validateBinding(binding, allowLocal); err != nil {
		return err
	}
	prepared, section, err := rcloneSpec(binding, session.program, session.proxyURL(), session.obscure)
	if err != nil {
		return err
	}
	if err := os.WriteFile(session.configPath, []byte(section), 0o600); err != nil {
		return errors.New("cannot write private transport configuration")
	}
	session.prepared[binding.Name] = prepared
	return nil
}

func (session *Session) Close() {
	if session == nil {
		return
	}
	session.closeOnce.Do(func() {
		if session.proxy != nil {
			session.proxy.Close()
		}
	})
}

func (session *Session) proxyURL() string {
	if session.proxy == nil {
		return ""
	}
	return session.proxy.URL
}

func supportsNative(connection Connection) bool {
	return connection.Driver() == DriverLocal || connection.Driver() == DriverS3
}

func PrepareNative(connection Connection, allowLocal bool) (PreparedRepository, []string, error) {
	if err := connection.Validate(allowLocal); err != nil {
		return PreparedRepository{}, nil, err
	}
	return nativeSpec(connection)
}

type RcloneOptions struct {
	Name            string
	Program         string
	ProxyURL        string
	AllowLocal      bool
	TrustOnFirstUse bool
	Obscure         func(string) (string, error)
}

func PrepareRclone(connection Connection, options RcloneOptions) (PreparedRepository, string, error) {
	binding := Binding{
		Name: options.Name, Connection: connection, Strategy: RequireRclone,
		TrustOnFirstUse: options.TrustOnFirstUse,
	}
	if err := validateBinding(binding, options.AllowLocal); err != nil {
		return PreparedRepository{}, "", err
	}
	if options.Obscure == nil {
		options.Obscure = func(string) (string, error) {
			return "", errors.New("password obscurer is unavailable")
		}
	}
	return rcloneSpec(binding, options.Program, options.ProxyURL, options.Obscure)
}

func validateBinding(binding Binding, allowLocal bool) error {
	if binding.TrustOnFirstUse {
		value, ok := binding.Connection.SFTP()
		if !ok || len(value.HostKeys) != 0 || binding.Strategy != RequireRclone {
			return errors.New("host keys must be empty for first-use trust")
		}
		if _, err := canonicalTarget(value.Host, value.Port); err != nil || !lineSafe(value.Username, 255) || !validSFTPPath(value.Path) || value.Authentication.Validate() != nil {
			return errors.New("invalid or missing SFTP settings")
		}
		return nil
	}
	return binding.Connection.Validate(allowLocal)
}

func nativeSpec(connection Connection) (PreparedRepository, []string, error) {
	switch value := connection.backend.(type) {
	case LocalConnection:
		return PreparedRepository{Repository: value.Path}, nil, nil
	case S3Connection:
		endpoint, err := canonicalS3Endpoint(value.Endpoint, value.Region)
		if err != nil {
			return PreparedRepository{}, nil, err
		}
		endpoint.Host = strings.TrimSuffix(endpoint.Host, ":443")
		endpoint.Path = "/" + value.Bucket + "/" + value.Prefix
		return PreparedRepository{
			Repository: "s3:" + endpoint.String(),
			Options:    []string{"s3.region=" + value.Region, "s3.bucket-lookup=path", "s3.retries=1"},
		}, []string{"AWS_ACCESS_KEY_ID=" + value.AccessKey, "AWS_SECRET_ACCESS_KEY=" + value.SecretKey}, nil
	default:
		return PreparedRepository{}, nil, errors.New("repository driver has no native Restic backend")
	}
}

func rcloneSpec(binding Binding, program, proxyURL string, obscure func(string) (string, error)) (PreparedRepository, string, error) {
	if !filepath.IsAbs(program) || strings.ContainsRune(program, 0) {
		return PreparedRepository{}, "", errors.New("repository transport requires the bundled rclone executable")
	}
	name := binding.Name
	switch value := binding.Connection.backend.(type) {
	case LocalConnection:
		return rclonePrepared(name, value.Path, program), fmt.Sprintf("[%s]\ntype = local\n", name), nil
	case S3Connection:
		endpoint, err := canonicalS3Endpoint(value.Endpoint, value.Region)
		if err != nil {
			return PreparedRepository{}, "", err
		}
		endpoint.Host = strings.TrimSuffix(endpoint.Host, ":443")
		section := fmt.Sprintf(
			"[%s]\ntype = s3\nprovider = Other\nenv_auth = false\naccess_key_id = %s\nsecret_access_key = %s\nendpoint = %s\nregion = %s\nforce_path_style = true\nno_check_bucket = true\n",
			name, value.AccessKey, value.SecretKey, endpoint.String(), value.Region,
		)
		return rclonePrepared(name, value.Bucket+"/"+value.Prefix, program), section, nil
	case SFTPConnection:
		target, err := canonicalTarget(value.Host, value.Port)
		if err != nil {
			return PreparedRepository{}, "", err
		}
		authentication := ""
		switch value.Authentication.Method() {
		case "password":
			password, obscureErr := obscure(value.Authentication.Secret())
			if obscureErr != nil || !lineSafe(password, 4096) {
				return PreparedRepository{}, "", errors.New("cannot prepare SFTP authentication")
			}
			authentication = "pass = " + password + "\n"
		case "ed25519":
			authentication = "key_pem = " + strings.ReplaceAll(strings.TrimSpace(value.Authentication.Secret()), "\n", `\n`) + "\n"
		default:
			return PreparedRepository{}, "", errors.New("invalid or missing SFTP settings")
		}
		section := fmt.Sprintf(
			"[%s]\ntype = sftp\nhost = %s\nport = %d\nuser = %s\n%sshell_type = none\ndisable_hashcheck = true\nhost_keys = %s\nhttp_proxy = %s\n",
			name, target.Host, target.Port, value.Username, authentication, strings.Join(value.HostKeys, ","), proxyURL,
		)
		if binding.TrustOnFirstUse {
			section += "pin_host_key = true\n"
		}
		return rclonePrepared(name, value.Path, program), section, nil
	default:
		return PreparedRepository{}, "", errors.New("unsupported repository driver")
	}
}

func rclonePrepared(name, path, program string) PreparedRepository {
	return PreparedRepository{Repository: "rclone:" + name + ":" + path, Options: []string{"rclone.program=" + program}}
}

func ParseHostKeys(configuration string) []string {
	for _, line := range strings.Split(configuration, "\n") {
		key, value, found := strings.Cut(line, "=")
		if found && strings.TrimSpace(key) == "host_keys" {
			parts := strings.Split(strings.TrimSpace(value), ",")
			keys := make([]string, 0, len(parts))
			for _, part := range parts {
				if part = strings.TrimSpace(part); part != "" {
					keys = append(keys, part)
				}
			}
			return keys
		}
	}
	return nil
}

func validRemoteName(value string) bool {
	return value != "" && !strings.ContainsAny(value, "[]\r\n\x00")
}

func proxyEnvironment(proxyURL string) []string {
	return []string{
		"HTTPS_PROXY=" + proxyURL, "HTTP_PROXY=" + proxyURL, "NO_PROXY=",
		"https_proxy=" + proxyURL, "http_proxy=" + proxyURL, "no_proxy=",
	}
}

func commandEnvironment(work string, extra []string) []string {
	environment := []string{"PATH=/usr/bin:/bin", "HOME=" + work, "TMPDIR=" + work, "LANG=C"}
	return appendUnique(environment, extra...)
}

func appendUnique(values []string, additions ...string) []string {
	seen := make(map[string]struct{}, len(values)+len(additions))
	for _, value := range values {
		seen[value] = struct{}{}
	}
	for _, value := range additions {
		if _, exists := seen[value]; !exists {
			values = append(values, value)
			seen[value] = struct{}{}
		}
	}
	return values
}
