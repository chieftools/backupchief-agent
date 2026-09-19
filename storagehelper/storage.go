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
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/chieftools/backupchief-agent/repository"
)

const (
	ProtocolVersion  = 1
	publicKeyComment = "storage@backup.chief.app"
)

type Request struct {
	Version         int                   `json:"version"`
	Operation       string                `json:"operation"`
	Connection      repository.Connection `json:"connection,omitempty"`
	RelativePath    string                `json:"relative_path,omitempty"`
	Contents        string                `json:"contents,omitempty"`
	TrustOnFirstUse bool                  `json:"trust_on_first_use,omitempty"`
	TimeoutSeconds  int                   `json:"timeout_seconds,omitempty"`
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
	sftp, sftpConnection := request.Connection.SFTP()
	if request.TimeoutSeconds < 1 || request.TimeoutSeconds > 3600 || !sftpConnection {
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

	binding := repository.Binding{
		Name: "backupchief_storage", Connection: request.Connection,
		Strategy: repository.RequireRclone, TrustOnFirstUse: request.TrustOnFirstUse,
	}
	session, err := repository.OpenSession(ctx, repository.SessionOptions{State: state, Work: work}, []repository.Binding{binding})
	if err != nil {
		result.Diagnostic = err.Error()
		return result
	}
	defer session.Close()

	run := func(arguments []string, input string) ([]byte, error) {
		base := []string{"--contimeout", "10s", "--timeout", "30s", "--retries", "1", "--low-level-retries", "1"}
		command, commandErr := session.RcloneCommand(ctx, append(base, arguments...)...)
		if commandErr != nil {
			return nil, commandErr
		}
		command.Stdin = strings.NewReader(input)
		return command.Output()
	}

	if request.TrustOnFirstUse {
		if _, err = run([]string{"lsd", remote("/")}, ""); err != nil {
			result.Diagnostic = "SFTP connection or first-use host-key discovery failed"
			return result
		}
		configurationBytes, readErr := os.ReadFile(session.RcloneConfigPath())
		if readErr != nil {
			result.Diagnostic = "cannot read discovered SFTP host keys"
			return result
		}
		sftp.HostKeys = repository.ParseHostKeys(string(configurationBytes))
		if len(sftp.HostKeys) == 0 {
			result.Diagnostic = "SFTP server did not provide a host key"
			return result
		}
		request.Connection = repository.NewSFTPConnection(sftp)
		binding.Connection = request.Connection
		binding.TrustOnFirstUse = false
		if err = session.RewriteRclone(binding, false); err != nil {
			result.Diagnostic = "cannot pin discovered SFTP host keys"
			return result
		}
	}

	switch request.Operation {
	case "verify":
		if _, err = run([]string{"mkdir", remote(sftp.Path)}, ""); err != nil {
			result.Diagnostic = "SFTP root path is unavailable"
			return result
		}
		token := make([]byte, 24)
		if _, err = rand.Read(token); err != nil {
			result.Diagnostic = "cannot create storage verification token"
			return result
		}
		proof := base64.RawURLEncoding.EncodeToString(token)
		proofPath := path.Join(sftp.Path, ".backupchief-verify-"+proof)
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
		if _, err = run([]string{"rcat", remote(path.Join(sftp.Path, request.RelativePath))}, request.Contents); err != nil {
			result.Diagnostic = "SFTP file write failed"
			return result
		}
	case "delete_directory":
		if !safeRelativePath(request.RelativePath) {
			result.Diagnostic = "invalid SFTP directory request"
			return result
		}
		if _, err = run([]string{"purge", remote(path.Join(sftp.Path, request.RelativePath))}, ""); err != nil {
			result.Diagnostic = "SFTP directory deletion failed"
			return result
		}
	default:
		result.Diagnostic = "unsupported storage operation"
		return result
	}

	result.Outcome = "complete"
	result.HostKeys = append([]string(nil), sftp.HostKeys...)
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
