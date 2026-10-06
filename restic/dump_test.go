package restic

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestFileDumpRequestBoundaries(t *testing.T) {
	target := filepath.Join(t.TempDir(), "file")
	request := testRequest(t)
	request.Operation = "dump_file"
	request.Snapshot = strings.Repeat("c", 64)
	request.Path = "/srv/site/wp-config.php"
	request.Target = target
	request.MaxBytes = 1 << 20

	args, _, err := request.arguments("password", "", "cache", true)
	want := []string{"dump", request.Snapshot + ":/srv/site", "/wp-config.php"}
	if err != nil || !reflect.DeepEqual(args[len(args)-len(want):], want) {
		t.Fatalf("dump arguments: %v %v", args, err)
	}

	for name, mutate := range map[string]func(*Request){
		"root path":       func(r *Request) { r.Path = "/" },
		"relative path":   func(r *Request) { r.Path = "srv/site" },
		"relative target": func(r *Request) { r.Target = "file" },
		"missing parent":  func(r *Request) { r.Target = filepath.Join(t.TempDir(), "missing", "file") },
		"no byte limit":   func(r *Request) { r.MaxBytes = 0 },
		"too many bytes":  func(r *Request) { r.MaxBytes = maxDumpBytes + 1 },
		"short snapshot":  func(r *Request) { r.Snapshot = "short" },
	} {
		candidate := request
		mutate(&candidate)
		if _, _, err := candidate.arguments("password", "", "cache", true); err == nil {
			t.Fatalf("accepted %s", name)
		}
	}
}

func TestFileDumpWritesOneFileWithinItsCap(t *testing.T) {
	runner := testRunner(t)
	request := testRequest(t)
	requireComplete(t, runner, request)

	root := filepath.Join(t.TempDir(), "synthetic dump")
	contents := bytes.Repeat([]byte("<?php define('DB_NAME', 'synthetic');\n"), 64)
	if err := os.MkdirAll(filepath.Join(root, "site"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "site", "wp-config.php"), contents, 0o600); err != nil {
		t.Fatal(err)
	}

	request.Operation = "backup"
	request.Root = root
	result := requireComplete(t, runner, request)

	var snapshotID string
	for _, line := range strings.Split(result.Output, "\n") {
		var summary struct {
			MessageType string `json:"message_type"`
			SnapshotID  string `json:"snapshot_id"`
		}
		if json.Unmarshal([]byte(line), &summary) == nil && summary.MessageType == "summary" {
			snapshotID = summary.SnapshotID
		}
	}

	target := filepath.Join(t.TempDir(), "dumped")
	request.Operation = "dump_file"
	request.Snapshot = snapshotID
	request.Path = filepath.Join(root, "site", "wp-config.php")
	request.Target = target
	request.MaxBytes = int64(len(contents))
	requireComplete(t, runner, request)

	dumped, err := os.ReadFile(target)
	if err != nil || !bytes.Equal(dumped, contents) {
		t.Fatalf("dumped bytes differ: %v", err)
	}
	if info, _ := os.Stat(target); info.Mode().Perm() != 0o600 {
		t.Fatalf("dump target is not private: %v", info.Mode())
	}

	result = runner.Run(context.Background(), request)
	if result.Outcome != "failed" || result.Diagnostic != "cannot create private file dump target" {
		t.Fatalf("overwrote an existing target: %+v", result)
	}

	capped := filepath.Join(t.TempDir(), "capped")
	request.Target = capped
	request.MaxBytes = int64(len(contents)) - 1
	result = runner.Run(context.Background(), request)
	if result.Outcome != "failed" || !result.Truncated || result.Diagnostic != "file dump exceeded its byte limit" {
		t.Fatalf("over-limit dump: %+v", result)
	}
	if _, err := os.Stat(capped); !os.IsNotExist(err) {
		t.Fatalf("over-limit dump left a partial file: %v", err)
	}
}
