package storagehelper

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"strings"
	"testing"
)

func TestGenerateEd25519ProducesMatchingOpenSSHAndPKCS8Keys(t *testing.T) {
	result := generateEd25519()
	if result.Outcome != "complete" || !strings.HasPrefix(result.PublicKey, "ssh-ed25519 ") || !strings.HasPrefix(result.Fingerprint, "SHA256:") {
		t.Fatalf("result: %+v", result)
	}
	block, trailing := pem.Decode([]byte(result.PrivateKey))
	if block == nil || block.Type != "PRIVATE KEY" || len(strings.TrimSpace(string(trailing))) != 0 {
		t.Fatal("private key is not a single PKCS#8 PEM block")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	privateKey, ok := parsed.(ed25519.PrivateKey)
	if !ok {
		t.Fatal("private key is not Ed25519")
	}
	publicKeyFields := strings.Fields(result.PublicKey)
	if len(publicKeyFields) != 3 || publicKeyFields[0] != "ssh-ed25519" || publicKeyFields[2] != "storage@backup.chief.app" {
		t.Fatalf("public key: %q", result.PublicKey)
	}
	encodedPublicKey, err := base64.StdEncoding.DecodeString(publicKeyFields[1])
	if err != nil {
		t.Fatal(err)
	}
	if string(encodedPublicKey) != string(sshPublicKey(privateKey.Public().(ed25519.PublicKey))) {
		t.Fatal("public and private keys do not match")
	}
}

func TestParseHostKeysReadsThePinnedRcloneSetting(t *testing.T) {
	configuration := "[backupchief_storage]\ntype = sftp\nhost_keys = ssh-ed25519 c3ludGhldGljLW9uZQ==,ssh-rsa c3ludGhldGljLXR3bw==\n"
	want := []string{"ssh-ed25519 c3ludGhldGljLW9uZQ==", "ssh-rsa c3ludGhldGljLXR3bw=="}
	got := parseHostKeys(configuration)
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("host keys: %v", got)
	}
}
