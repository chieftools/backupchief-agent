package repository

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
	"strings"
	"unicode/utf8"

	"github.com/chieftools/backupchief-agent/egress"
)

type Driver string

const (
	DriverLocal Driver = "local"
	DriverS3    Driver = "s3"
	DriverSFTP  Driver = "sftp"
)

var bucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

var amazonS3EndpointPattern = regexp.MustCompile(`^s3[.-]([a-z0-9-]+)\.amazonaws\.com$`)

type Destination struct {
	backend destinationBackend
}

type destinationBackend interface {
	driver() Driver
	validate() error
	resolve(string) (Connection, error)
}

type LocalDestination struct {
	Root string
}

type S3Destination struct {
	Endpoint  string
	Region    string
	Bucket    string
	Prefix    string
	AccessKey string
	SecretKey string
}

type SFTPDestination struct {
	Host           string
	Port           uint16
	Username       string
	RootPath       string
	HostKeys       []string
	Authentication SFTPAuthentication
}

func NewLocalDestination(root string) Destination {
	return Destination{backend: LocalDestination{Root: root}}
}

func NewS3Destination(value S3Destination) Destination {
	return Destination{backend: value}
}

func NewSFTPDestination(value SFTPDestination) Destination {
	value.HostKeys = append([]string(nil), value.HostKeys...)
	return Destination{backend: value}
}

func (destination Destination) Driver() Driver {
	if destination.backend == nil {
		return ""
	}

	return destination.backend.driver()
}

func (destination Destination) Validate() error {
	if destination.backend == nil {
		return errors.New("driver is invalid")
	}

	return destination.backend.validate()
}

func (destination Destination) Resolve(relativePath string) (Connection, error) {
	if destination.backend == nil {
		return Connection{}, errors.New("driver is invalid")
	}
	if !validRelativePath(relativePath) || runeLength(relativePath) > 1024 {
		return Connection{}, errors.New("repository path must be a safe relative path")
	}

	return destination.backend.resolve(relativePath)
}

func (destination Destination) Local() (LocalDestination, bool) {
	value, ok := destination.backend.(LocalDestination)
	return value, ok
}

func (destination Destination) S3() (S3Destination, bool) {
	value, ok := destination.backend.(S3Destination)
	return value, ok
}

func (destination Destination) SFTP() (SFTPDestination, bool) {
	value, ok := destination.backend.(SFTPDestination)
	value.HostKeys = append([]string(nil), value.HostKeys...)
	return value, ok
}

func (value LocalDestination) driver() Driver { return DriverLocal }

func (value LocalDestination) validate() error {
	if !filepath.IsAbs(value.Root) || strings.ContainsRune(value.Root, 0) || runeLength(value.Root) > 4096 {
		return errors.New("local settings are invalid")
	}

	return nil
}

func (value LocalDestination) resolve(relativePath string) (Connection, error) {
	connection := NewLocalConnection(filepath.Join(value.Root, filepath.FromSlash(relativePath)))
	return connection, nil
}

func (value S3Destination) driver() Driver { return DriverS3 }

func (value S3Destination) validate() error {
	if _, err := canonicalS3Endpoint(value.Endpoint, value.Region); err != nil ||
		!bucketPattern.MatchString(value.Bucket) || value.Region == "" || runeLength(value.Region) > 255 ||
		(value.Prefix != "" && !validRelativePath(value.Prefix)) || runeLength(value.Prefix) > 1024 ||
		!lineSafe(value.AccessKey, 1000) || !lineSafe(value.SecretKey, 1000) {
		return errors.New("S3 settings are invalid")
	}
	return nil
}

func (value S3Destination) resolve(relativePath string) (Connection, error) {
	prefix := relativePath
	if value.Prefix != "" {
		prefix = value.Prefix + "/" + relativePath
	}

	return NewS3Connection(S3Connection{
		Endpoint: value.Endpoint, Region: value.Region, Bucket: value.Bucket, Prefix: prefix,
		AccessKey: value.AccessKey, SecretKey: value.SecretKey,
	}), nil
}

func (value SFTPDestination) driver() Driver { return DriverSFTP }

func (value SFTPDestination) validate() error {
	if _, err := canonicalTarget(value.Host, value.Port); err != nil || !lineSafe(value.Username, 255) ||
		!validSFTPPath(value.RootPath) || len(value.HostKeys) == 0 || len(value.HostKeys) > 16 {
		return errors.New("SFTP settings are invalid")
	}
	if err := validateHostKeys(value.HostKeys); err != nil {
		return err
	}

	return value.Authentication.Validate()
}

func (value SFTPDestination) resolve(relativePath string) (Connection, error) {
	path := value.RootPath + "/" + relativePath
	if value.RootPath == "/" {
		path = "/" + relativePath
	}

	return NewSFTPConnection(SFTPConnection{
		Host: value.Host, Port: value.Port, Username: value.Username, Path: path,
		HostKeys: value.HostKeys, Authentication: value.Authentication,
	}), nil
}

type Connection struct {
	backend connectionBackend
}

type connectionBackend interface {
	driver() Driver
}

type LocalConnection struct {
	Path string
}

type S3Connection struct {
	Endpoint  string
	Bucket    string
	Prefix    string
	Region    string
	AccessKey string
	SecretKey string
}

type SFTPConnection struct {
	Host           string
	Port           uint16
	Username       string
	Path           string
	HostKeys       []string
	Authentication SFTPAuthentication
}

func NewLocalConnection(path string) Connection {
	return Connection{backend: LocalConnection{Path: path}}
}

func NewS3Connection(value S3Connection) Connection {
	return Connection{backend: value}
}

func NewSFTPConnection(value SFTPConnection) Connection {
	value.HostKeys = append([]string(nil), value.HostKeys...)
	return Connection{backend: value}
}

func (connection Connection) Driver() Driver {
	if connection.backend == nil {
		return ""
	}

	return connection.backend.driver()
}

func (connection Connection) Local() (LocalConnection, bool) {
	value, ok := connection.backend.(LocalConnection)
	return value, ok
}

func (connection Connection) S3() (S3Connection, bool) {
	value, ok := connection.backend.(S3Connection)
	return value, ok
}

func (connection Connection) SFTP() (SFTPConnection, bool) {
	value, ok := connection.backend.(SFTPConnection)
	value.HostKeys = append([]string(nil), value.HostKeys...)
	return value, ok
}

func (connection Connection) RepositoryPath() string {
	switch value := connection.backend.(type) {
	case LocalConnection:
		return value.Path
	case S3Connection:
		return value.Prefix
	case SFTPConnection:
		return value.Path
	default:
		return ""
	}
}

func (connection Connection) Validate(allowLocal bool) error {
	switch value := connection.backend.(type) {
	case LocalConnection:
		if !allowLocal || !filepath.IsAbs(value.Path) || strings.ContainsRune(value.Path, 0) {
			return errors.New("local repositories require an absolute path and local execution permission")
		}
	case S3Connection:
		if _, err := canonicalS3Endpoint(value.Endpoint, value.Region); err != nil || !bucketPattern.MatchString(value.Bucket) ||
			value.Region == "" || !safePrefix(value.Prefix) || !lineSafe(value.AccessKey, 1000) || !lineSafe(value.SecretKey, 1000) {
			return errors.New("invalid or missing S3 settings")
		}
	case SFTPConnection:
		if _, err := canonicalTarget(value.Host, value.Port); err != nil || !lineSafe(value.Username, 255) ||
			!validSFTPPath(value.Path) || len(value.HostKeys) == 0 || len(value.HostKeys) > 16 {
			return errors.New("invalid or missing SFTP settings")
		}
		if err := validateHostKeys(value.HostKeys); err != nil || value.Authentication.Validate() != nil {
			return errors.New("invalid or missing SFTP settings")
		}
	default:
		return errors.New("unsupported repository driver")
	}

	return nil
}

func (connection Connection) Location() string {
	switch value := connection.backend.(type) {
	case LocalConnection:
		return value.Path
	case S3Connection:
		return "s3:" + value.Endpoint + "/" + value.Bucket + "/" + value.Prefix
	case SFTPConnection:
		target, err := canonicalTarget(value.Host, value.Port)
		if err != nil {
			return ""
		}

		prefix := fmt.Sprintf("sftp://%s@%s/", url.User(value.Username).String(), target.Authority)
		if strings.HasPrefix(value.Path, "/") {
			return prefix + "/" + strings.TrimPrefix(value.Path, "/")
		}

		return prefix + value.Path
	default:
		return ""
	}
}

// Identity excludes credentials and trust material so one repository always uses one lock.
func (connection Connection) Identity() string {
	switch value := connection.backend.(type) {
	case LocalConnection:
		return "local\x00" + filepath.Clean(value.Path)
	case S3Connection:
		endpoint, _ := canonicalS3Endpoint(value.Endpoint, value.Region)
		canonical := value.Endpoint
		if endpoint != nil {
			canonical = endpoint.String()
		}

		return "s3\x00" + canonical + "\x00" + value.Bucket + "\x00" + value.Prefix
	case SFTPConnection:
		target, _ := canonicalTarget(value.Host, value.Port)
		return "sftp\x00" + target.Authority + "\x00" + value.Username + "\x00" + value.Path
	default:
		return ""
	}
}

func (connection Connection) Secrets() []string {
	switch value := connection.backend.(type) {
	case S3Connection:
		return []string{value.AccessKey, value.SecretKey}
	case SFTPConnection:
		return []string{value.Authentication.Secret()}
	default:
		return nil
	}
}

func (connection Connection) Target() (egress.Target, bool, error) {
	switch value := connection.backend.(type) {
	case LocalConnection:
		return egress.Target{}, false, nil
	case S3Connection:
		endpoint, err := canonicalS3Endpoint(value.Endpoint, value.Region)
		if err != nil {
			return egress.Target{}, false, err
		}

		return egress.Target{Host: endpoint.Hostname(), Port: 443}, true, nil
	case SFTPConnection:
		target, err := canonicalTarget(value.Host, value.Port)
		return target.Target, true, err
	default:
		return egress.Target{}, false, errors.New("unsupported repository driver")
	}
}

func (connection Connection) Decompose() (Destination, string, error) {
	switch value := connection.backend.(type) {
	case LocalConnection:
		return NewLocalDestination(filepath.Dir(value.Path)), filepath.Base(value.Path), nil
	case S3Connection:
		return NewS3Destination(S3Destination{
			Endpoint: value.Endpoint, Region: value.Region, Bucket: value.Bucket,
			AccessKey: value.AccessKey, SecretKey: value.SecretKey,
		}), value.Prefix, nil
	case SFTPConnection:
		return NewSFTPDestination(SFTPDestination{
			Host: value.Host, Port: value.Port, Username: value.Username,
			RootPath: pathDir(value.Path), HostKeys: value.HostKeys, Authentication: value.Authentication,
		}), pathBase(value.Path), nil
	default:
		return Destination{}, "", errors.New("unsupported repository driver")
	}
}

func (LocalConnection) driver() Driver { return DriverLocal }
func (S3Connection) driver() Driver    { return DriverS3 }
func (SFTPConnection) driver() Driver  { return DriverSFTP }

type SFTPAuthentication struct {
	credential sftpCredential
}

type sftpCredential interface {
	method() string
	secret() string
	validate() error
}

type passwordCredential string
type ed25519Credential string

func PasswordAuthentication(password string) SFTPAuthentication {
	return SFTPAuthentication{credential: passwordCredential(password)}
}

func Ed25519Authentication(privateKey string) SFTPAuthentication {
	return SFTPAuthentication{credential: ed25519Credential(privateKey)}
}

func (authentication SFTPAuthentication) Method() string {
	if authentication.credential == nil {
		return ""
	}

	return authentication.credential.method()
}

func (authentication SFTPAuthentication) Secret() string {
	if authentication.credential == nil {
		return ""
	}

	return authentication.credential.secret()
}

func (authentication SFTPAuthentication) Validate() error {
	if authentication.credential == nil {
		return errors.New("SFTP authentication method is invalid")
	}

	return authentication.credential.validate()
}

func (value passwordCredential) method() string { return "password" }
func (value passwordCredential) secret() string { return string(value) }
func (value passwordCredential) validate() error {
	if !lineSafe(string(value), 1000) {
		return errors.New("SFTP password authentication is invalid")
	}

	return nil
}

func (value ed25519Credential) method() string { return "ed25519" }
func (value ed25519Credential) secret() string { return string(value) }
func (value ed25519Credential) validate() error {
	if !validEd25519PrivateKey(string(value)) {
		return errors.New("SFTP Ed25519 authentication is invalid")
	}

	return nil
}

type canonicalEgressTarget struct {
	egress.Target
	Authority string
}

func canonicalTarget(host string, port uint16) (canonicalEgressTarget, error) {
	if host != strings.TrimSpace(host) {
		return canonicalEgressTarget{}, errors.New("invalid storage hostname")
	}

	authority, err := egress.Authority(egress.Target{Host: host, Port: port})
	if err != nil {
		return canonicalEgressTarget{}, err
	}

	canonicalHost, _, err := net.SplitHostPort(authority)
	if err != nil {
		return canonicalEgressTarget{}, err
	}

	return canonicalEgressTarget{Target: egress.Target{Host: canonicalHost, Port: port}, Authority: authority}, nil
}

func canonicalS3Endpoint(raw, region string) (*url.URL, error) {
	endpoint, err := egress.Endpoint(raw)
	if err != nil {
		return nil, err
	}

	// AWS regional endpoints use dual-stack DNS so the guarded proxy can retain IPv6 reachability.
	matches := amazonS3EndpointPattern.FindStringSubmatch(endpoint.Hostname())
	if len(matches) == 2 && matches[1] == region {
		endpoint.Host = net.JoinHostPort("s3.dualstack."+region+".amazonaws.com", "443")
	}

	return endpoint, nil
}

func validRelativePath(value string) bool {
	if value == "" || strings.ContainsAny(value, "\\\x00") || strings.HasPrefix(value, "/") {
		return false
	}

	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}

	return true
}

func safePrefix(value string) bool {
	return value != "" && validRelativePath(value)
}

func validSFTPPath(value string) bool {
	if value == "" || runeLength(value) > 1024 || strings.ContainsAny(value, "\\\x00") || value == "~" || strings.HasPrefix(value, "~/") {
		return false
	}
	if value == "/" {
		return true
	}
	if strings.HasSuffix(value, "/") || strings.Contains(value, "//") {
		return false
	}

	for _, part := range strings.Split(strings.TrimPrefix(value, "/"), "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}

	return true
}

func validateHostKeys(values []string) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists || !validSFTPHostKey(value) {
			return errors.New("SFTP host keys are invalid")
		}

		seen[value] = struct{}{}
	}

	return nil
}

func validSFTPHostKey(value string) bool {
	if value == "" || len(value) > 4096 || strings.ContainsAny(value, "\r\n\x00") {
		return false
	}

	parts := strings.Fields(value)
	if len(parts) != 2 || strings.Join(parts, " ") != value || !lineSafe(parts[0], 255) {
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

func lineSafe(value string, maximum int) bool {
	return value != "" && runeLength(value) <= maximum && !strings.ContainsAny(value, "\r\n\x00")
}

func runeLength(value string) int { return utf8.RuneCountInString(value) }

func pathDir(value string) string {
	index := strings.LastIndex(value, "/")
	if index < 0 {
		return "."
	}
	if index == 0 {
		return "/"
	}

	return value[:index]
}

func pathBase(value string) string {
	index := strings.LastIndex(value, "/")
	if index < 0 {
		return value
	}

	return value[index+1:]
}
