package repository

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"path/filepath"
	"strings"
	"testing"
)

func TestDestinationResolveAndDecompose(t *testing.T) {
	destination := NewSFTPDestination(SFTPDestination{
		Host: "Archive.Example.Test", Port: 2222, Username: "synthetic-backup", RootPath: "/repositories",
		HostKeys:       []string{"ssh-ed25519 c3ludGhldGljLWhvc3Qta2V5"},
		Authentication: PasswordAuthentication("synthetic-storage-password"),
	})
	if err := destination.Validate(); err != nil {
		t.Fatal(err)
	}
	connection, err := destination.Resolve("documents/repository")
	if err != nil {
		t.Fatal(err)
	}
	if connection.Location() != "sftp://synthetic-backup@archive.example.test:2222//repositories/documents/repository" {
		t.Fatalf("location: %s", connection.Location())
	}
	resolved, ok := connection.SFTP()
	if !ok || resolved.Path != "/repositories/documents/repository" {
		t.Fatalf("connection: %+v", resolved)
	}
	decomposed, relativePath, err := connection.Decompose()
	if err != nil || relativePath != "repository" {
		t.Fatalf("decompose: %q %v", relativePath, err)
	}
	sftp, ok := decomposed.SFTP()
	if !ok || sftp.RootPath != "/repositories/documents" {
		t.Fatalf("destination: %+v", sftp)
	}
}

func TestConnectionJSONKeepsReleasedShapesAndNestsSFTPAuthentication(t *testing.T) {
	local, err := json.Marshal(NewLocalConnection("/srv/synthetic-repository"))
	if err != nil || string(local) != `{"driver":"local","path":"/srv/synthetic-repository"}` {
		t.Fatalf("local: %s %v", local, err)
	}

	s3, err := json.Marshal(NewS3Connection(S3Connection{
		Endpoint: "https://objects.example.test", Bucket: "synthetic-bucket", Prefix: "jobs/repository",
		Region: "test-1", AccessKey: "synthetic-access", SecretKey: "synthetic-secret",
	}))
	if err != nil || !strings.Contains(string(s3), `"access_key":"synthetic-access"`) || strings.Contains(string(s3), `"auth"`) {
		t.Fatalf("S3: %s %v", s3, err)
	}

	sftp, err := json.Marshal(NewSFTPConnection(SFTPConnection{
		Host: "archive.example.test", Port: 22, Username: "synthetic", Path: "/repository",
		HostKeys: []string{"ssh-ed25519 c3ludGhldGljLWhvc3Qta2V5"}, Authentication: PasswordAuthentication("synthetic-password"),
	}))
	if err != nil || !strings.Contains(string(sftp), `"auth":{"method":"password","password":"synthetic-password"}`) || strings.Contains(string(sftp), `"private_key"`) {
		t.Fatalf("SFTP: %s %v", sftp, err)
	}
}

func TestConnectionDecoderRejectsUnknownAndCrossDriverFields(t *testing.T) {
	for _, document := range []string{
		`{"driver":"local","path":"/srv/synthetic","future":true}`,
		`{"driver":"local","path":"/srv/synthetic","bucket":"synthetic-bucket"}`,
		`{"driver":"sftp","path":"/repository","host":"archive.example.test","port":22,"username":"synthetic","host_keys":["ssh-ed25519 c3ludGhldGljLWhvc3Qta2V5"],"auth":{"method":"password","password":"synthetic","future":true}}`,
	} {
		var connection Connection
		if err := json.Unmarshal([]byte(document), &connection); err == nil {
			t.Fatalf("accepted %s", document)
		}
	}
}

func TestDestinationDecoderIgnoresFutureFieldsButRejectsKnownCrossDriverFields(t *testing.T) {
	destination, err := DecodeDestination([]byte(`{"driver":"local","path":"/srv/synthetic","future_mode":true}`))
	if err != nil || destination.Driver() != DriverLocal {
		t.Fatalf("future field: %v", err)
	}
	if _, err := DecodeDestination([]byte(`{"driver":"local","path":"/srv/synthetic","bucket":"synthetic-bucket"}`)); err == nil {
		t.Fatal("accepted S3 field on local destination")
	}
}

func TestRepositoryIdentityExcludesCredentialsAndHostKeys(t *testing.T) {
	first := NewS3Connection(S3Connection{
		Endpoint: "https://objects.example.test", Bucket: "synthetic-bucket", Prefix: "repository",
		Region: "test-1", AccessKey: "synthetic-access-one", SecretKey: "synthetic-secret-one",
	})
	second := NewS3Connection(S3Connection{
		Endpoint: "https://objects.example.test", Bucket: "synthetic-bucket", Prefix: "repository",
		Region: "test-2", AccessKey: "synthetic-access-two", SecretKey: "synthetic-secret-two",
	})
	if first.Identity() != second.Identity() {
		t.Fatalf("credentials changed identity: %q %q", first.Identity(), second.Identity())
	}
	different := NewS3Connection(S3Connection{Endpoint: "https://objects.example.test", Bucket: "synthetic-bucket", Prefix: "other", Region: "test-1"})
	if first.Identity() == different.Identity() {
		t.Fatal("different repository paths share an identity")
	}
}

func TestSFTPRcloneSpecUsesCanonicalTargetAndAuthentication(t *testing.T) {
	connection := NewSFTPConnection(SFTPConnection{
		Host: "Archive.Example.Test", Port: 2222, Username: "synthetic", Path: "/repository",
		HostKeys: []string{"ssh-ed25519 c3ludGhldGljLWhvc3Qta2V5"}, Authentication: PasswordAuthentication("synthetic-password"),
	})
	prepared, configuration, err := PrepareRclone(connection, RcloneOptions{
		Name: "synthetic_remote", Program: "/private/runtime/rclone", ProxyURL: "http://127.0.0.1:43123",
		Obscure: func(value string) (string, error) {
			if value != "synthetic-password" {
				t.Fatalf("password: %q", value)
			}
			return "obscured-synthetic", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Repository != "rclone:synthetic_remote:/repository" || !strings.Contains(configuration, "host = archive.example.test\n") || !strings.Contains(configuration, "pass = obscured-synthetic\n") {
		t.Fatalf("prepared=%+v configuration=%s", prepared, configuration)
	}
}

func TestSFTPValidationRejectsWhitespaceDuplicatesAndInvalidKey(t *testing.T) {
	privateKey := syntheticPrivateKey(t)
	for _, connection := range []Connection{
		NewSFTPConnection(SFTPConnection{Host: " archive.example.test", Port: 22, Username: "synthetic", Path: "/repository", HostKeys: []string{"ssh-ed25519 c3ludGhldGljLWhvc3Qta2V5"}, Authentication: Ed25519Authentication(privateKey)}),
		NewSFTPConnection(SFTPConnection{Host: "archive.example.test", Port: 22, Username: "synthetic", Path: "/repository", HostKeys: []string{"ssh-ed25519 c3ludGhldGljLWhvc3Qta2V5", "ssh-ed25519 c3ludGhldGljLWhvc3Qta2V5"}, Authentication: Ed25519Authentication(privateKey)}),
	} {
		if err := connection.Validate(false); err == nil {
			t.Fatal("accepted invalid SFTP connection")
		}
	}
}

func TestLocalSessionNeedsNoTransportAndClosesIdempotently(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	work := filepath.Join(t.TempDir(), "work")
	session, err := OpenSession(context.Background(), SessionOptions{State: state, Work: work, AllowLocal: true}, []Binding{{
		Name: "synthetic_repository", Connection: NewLocalConnection("/srv/synthetic-repository"), Strategy: PreferNative,
	}})
	if err != nil {
		t.Fatal(err)
	}
	prepared, ok := session.Repository("synthetic_repository")
	if !ok || prepared.Repository != "/srv/synthetic-repository" || session.RcloneConfigPath() != "" || len(session.Environment()) != 0 {
		t.Fatalf("session: prepared=%+v config=%q environment=%v", prepared, session.RcloneConfigPath(), session.Environment())
	}
	session.Close()
	session.Close()
}

func syntheticPrivateKey(t *testing.T) string {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded}))
}
