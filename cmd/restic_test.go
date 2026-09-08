package cmd

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chieftools/backupchief-agent/restic"
)

func TestResticLifetimePipe(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	request := restic.Request{
		Version:   1,
		Operation: "init",
		Connection: restic.Connection{
			Driver: "local",
			Path:   filepath.Join(t.TempDir(), "repository"),
		},
		Password:        "synthetic-pipe-password",
		TimeoutSeconds:  300,
		LockWaitSeconds: 0,
	}
	data, _ := json.Marshal(request)

	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()

	var output bytes.Buffer

	command := newResticCommand()
	args := []string{"--state", state, "--development-local"}

	command.SetArgs(args)
	command.SetIn(reader)
	command.SetOut(&output)

	done := make(chan error, 1)

	go func() {
		done <- command.Execute()
	}()

	if _, err := writer.Write(append(data, '\n')); err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("helper outlived the closed parent pipe")
	}

	var result restic.Result

	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Outcome == "complete" {
		t.Fatalf("executed after parent loss: %+v", result)
	}

	entries, _ := os.ReadDir(filepath.Join(state, "work"))
	if len(entries) != 0 {
		t.Fatal("parent loss abandoned local work")
	}
}

func TestResticRejectsUnknownFields(t *testing.T) {
	command := newResticCommand()
	command.SetIn(bytes.NewBufferString("{\"version\":1,\"shell\":\"synthetic\"}\n"))
	if err := command.Execute(); err == nil {
		t.Fatal("accepted unknown request field")
	}
}
