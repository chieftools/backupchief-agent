package cmd

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/chieftools/backupchief-agent/storagehelper"
)

func TestStorageCommandGeneratesAnEd25519Credential(t *testing.T) {
	command := newStorageCommand()
	command.SetArgs([]string{"--state", t.TempDir()})
	command.SetIn(strings.NewReader(`{"version":1,"operation":"generate_ed25519"}` + "\n"))
	var output bytes.Buffer
	command.SetOut(&output)

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	var result storagehelper.Result
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "complete" || !strings.HasPrefix(result.PublicKey, "ssh-ed25519 ") || !strings.HasPrefix(result.PrivateKey, "-----BEGIN PRIVATE KEY-----") {
		t.Fatalf("result: %+v", result)
	}
}

func TestStorageCommandRejectsUnknownRequestFields(t *testing.T) {
	command := newStorageCommand()
	command.SetIn(strings.NewReader(`{"version":1,"operation":"generate_ed25519","future_mode":true}` + "\n"))
	if err := command.Execute(); err == nil {
		t.Fatal("accepted unknown storage request field")
	}
}
