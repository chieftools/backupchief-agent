package storagehelper

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/chieftools/backupchief-agent/egress"
	bundledrclone "github.com/chieftools/backupchief-agent/rclone"
	"github.com/chieftools/backupchief-agent/restic"
)

const (
	ProtocolVersion  = 1
	publicKeyComment = "storage@backup.chief.app"
)

type Request struct {
	Version         int               `json:"version"`
	Operation       string            `json:"operation"`
	Connection      restic.Connection `json:"connection,omitempty"`
	RelativePath    string            `json:"relative_path,omitempty"`
	Contents        string            `json:"contents,omitempty"`
	TrustOnFirstUse bool              `json:"trust_on_first_use,omitempty"`
	TimeoutSeconds  int               `json:"timeout_seconds,omitempty"`
}

type Result struct {
	Version     int      `json:"version"`
	Outcome     string   `json:"outcome"`
	HostKeys    []string `json:"host_keys,omitempty"`
	PublicKey   string   `json:"public_key,omitempty"`
	PrivateKey  string   `json:"private_key,omitempty"`
	Fingerprint string   `json:"fingerprint,omitempty"`
	Diagnostic  string   `json:"diagnostic,omitempty"`
}

func Run(ctx context.Context, state string, request Request) Result {
	result := Result{Version: ProtocolVersion, Outcome: "failed"}
	if request.Version != ProtocolVersion {
		result.Diagnostic = "unsupported storage request version"
		return result
	}
	if request.Operation == "generate_ed25519" {
		return generateEd25519()
	}
	if request.TimeoutSeconds < 1 || request.TimeoutSeconds > 3600 || request.Connection.Driver != "sftp" {
		result.Diagnostic = "invalid storage request"
		return result
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(request.TimeoutSeconds)*time.Second)
	defer cancel()
	if err := privateDirectory(state); err != nil {
		result.Diagnostic = "invalid private state directory"
		return result
	}
	work, err := os.MkdirTemp(state, "storage-")
	if err != nil {
		result.Diagnostic = "cannot prepare private storage workspace"
		return result
	}
	defer os.RemoveAll(work)
	if err = os.Chmod(work, 0700); err != nil {
		result.Diagnostic = "cannot prepare private storage workspace"
		return result
	}

	program, err := bundledrclone.Binary(ctx, state)
	if err != nil {
		result.Diagnostic = "cannot prepare bundled rclone executable"
		return result
	}
	proxy, err := egress.StartTargets(ctx, []egress.Target{{Host: request.Connection.Host, Port: request.Connection.Port}})
	if err != nil {
		result.Diagnostic = "cannot establish guarded storage transport"
		return result
	}
	defer proxy.Close()

	obscuredPassword := ""
	if request.Connection.SFTPPassword != "" {
		obscuredPassword, err = obscure(ctx, program, work, request.Connection.SFTPPassword)
		if err != nil {
			result.Diagnostic = "cannot prepare SFTP authentication"
			return result
		}
	}
	_, configuration, err := restic.SFTPRemoteConfig("backupchief_storage", request.Connection, obscuredPassword, proxy.URL, request.TrustOnFirstUse)
	if err != nil {
		result.Diagnostic = err.Error()
		return result
	}
	if request.TrustOnFirstUse {
		if len(request.Connection.HostKeys) != 0 {
			result.Diagnostic = "host keys must be empty for first-use trust"
			return result
		}
		configuration += "pin_host_key = true\n"
	}
	configFile := filepath.Join(work, "rclone.conf")
	if err = os.WriteFile(configFile, []byte(configuration), 0600); err != nil {
		result.Diagnostic = "cannot write private transport configuration"
		return result
	}

	run := func(arguments []string, input string) ([]byte, error) {
		base := []string{"--config", configFile, "--contimeout", "10s", "--timeout", "30s", "--retries", "1", "--low-level-retries", "1"}
		command := exec.CommandContext(ctx, program, append(base, arguments...)...)
		command.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + work, "TMPDIR=" + work, "LANG=C"}
		command.Dir = work
		command.Stdin = strings.NewReader(input)
		return command.Output()
	}

	if request.TrustOnFirstUse {
		if _, err = run([]string{"lsd", remote("/")}, ""); err != nil {
			result.Diagnostic = "SFTP connection or first-use host-key discovery failed"
			return result
		}
		configurationBytes, readErr := os.ReadFile(configFile)
		if readErr != nil {
			result.Diagnostic = "cannot read discovered SFTP host keys"
			return result
		}
		request.Connection.HostKeys = parseHostKeys(string(configurationBytes))
		if len(request.Connection.HostKeys) == 0 {
			result.Diagnostic = "SFTP server did not provide a host key"
			return result
		}
		_, configuration, err = restic.SFTPRemoteConfig("backupchief_storage", request.Connection, obscuredPassword, proxy.URL, false)
		if err != nil || os.WriteFile(configFile, []byte(configuration), 0600) != nil {
			result.Diagnostic = "cannot pin discovered SFTP host keys"
			return result
		}
	}

	switch request.Operation {
	case "verify":
		if _, err = run([]string{"mkdir", remote(request.Connection.Path)}, ""); err != nil {
			result.Diagnostic = "SFTP root path is unavailable"
			return result
		}
		token := make([]byte, 24)
		if _, err = rand.Read(token); err != nil {
			result.Diagnostic = "cannot create storage verification token"
			return result
		}
		proof := base64.RawURLEncoding.EncodeToString(token)
		proofPath := path.Join(request.Connection.Path, ".backupchief-verify-"+proof)
		if _, err = run([]string{"rcat", remote(proofPath)}, proof); err != nil {
			result.Diagnostic = "SFTP write verification failed"
			return result
		}
		contents, readErr := run([]string{"cat", remote(proofPath)}, "")
		_, deleteErr := run([]string{"deletefile", remote(proofPath)}, "")
		if readErr != nil || string(contents) != proof || deleteErr != nil {
			result.Diagnostic = "SFTP read or delete verification failed"
			return result
		}
	case "put":
		if !safeRelativePath(request.RelativePath) || len(request.Contents) > 1<<20 {
			result.Diagnostic = "invalid SFTP file request"
			return result
		}
		if _, err = run([]string{"rcat", remote(path.Join(request.Connection.Path, request.RelativePath))}, request.Contents); err != nil {
			result.Diagnostic = "SFTP file write failed"
			return result
		}
	case "delete_directory":
		if !safeRelativePath(request.RelativePath) {
			result.Diagnostic = "invalid SFTP directory request"
			return result
		}
		if _, err = run([]string{"purge", remote(path.Join(request.Connection.Path, request.RelativePath))}, ""); err != nil {
			result.Diagnostic = "SFTP directory deletion failed"
			return result
		}
	default:
		result.Diagnostic = "unsupported storage operation"
		return result
	}

	result.Outcome = "complete"
	result.HostKeys = append([]string(nil), request.Connection.HostKeys...)
	return result
}

func generateEd25519() Result {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return Result{Version: ProtocolVersion, Outcome: "failed", Diagnostic: "cannot generate Ed25519 key"}
	}
	privateBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return Result{Version: ProtocolVersion, Outcome: "failed", Diagnostic: "cannot encode Ed25519 key"}
	}
	wire := sshPublicKey(publicKey)
	fingerprint := sha256.Sum256(wire)
	return Result{
		Version:     ProtocolVersion,
		Outcome:     "complete",
		PublicKey:   "ssh-ed25519 " + base64.StdEncoding.EncodeToString(wire) + " " + publicKeyComment,
		PrivateKey:  string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateBytes})),
		Fingerprint: "SHA256:" + base64.RawStdEncoding.EncodeToString(fingerprint[:]),
	}
}

func sshPublicKey(publicKey ed25519.PublicKey) []byte {
	algorithm := []byte("ssh-ed25519")
	wire := make([]byte, 4+len(algorithm)+4+len(publicKey))
	binary.BigEndian.PutUint32(wire, uint32(len(algorithm)))
	copy(wire[4:], algorithm)
	offset := 4 + len(algorithm)
	binary.BigEndian.PutUint32(wire[offset:], uint32(len(publicKey)))
	copy(wire[offset+4:], publicKey)
	return wire
}

func obscure(ctx context.Context, program, work, password string) (string, error) {
	command := exec.CommandContext(ctx, program, "obscure", "-")
	command.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + work, "TMPDIR=" + work, "LANG=C"}
	command.Dir = work
	command.Stdin = strings.NewReader(password)
	output, err := command.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func parseHostKeys(configuration string) []string {
	for _, line := range strings.Split(configuration, "\n") {
		key, value, found := strings.Cut(line, "=")
		if found && strings.TrimSpace(key) == "host_keys" {
			values := strings.Split(strings.TrimSpace(value), ",")
			keys := make([]string, 0, len(values))
			for _, value := range values {
				if value = strings.TrimSpace(value); value != "" {
					keys = append(keys, value)
				}
			}
			return keys
		}
	}
	return nil
}

func remote(remotePath string) string {
	return "backupchief_storage:" + remotePath
}

func safeRelativePath(value string) bool {
	if value == "" || len(value) > 1024 || strings.ContainsAny(value, "\\\x00") || strings.HasPrefix(value, "/") {
		return false
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

func privateDirectory(directory string) error {
	if !filepath.IsAbs(directory) || strings.ContainsRune(directory, 0) {
		return errors.New("state directory must be absolute")
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("state directory must be private")
	}
	return nil
}
