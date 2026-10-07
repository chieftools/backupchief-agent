package restic

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

type archivedEntry struct {
	Mode     os.FileMode
	Contents string
	Modified int64
}

func TestScratchExportMatchesTheStreamedDumpLayout(t *testing.T) {
	runner := testRunner(t)
	request := testRequest(t)
	requireComplete(t, runner, request)

	root := filepath.Join(t.TempDir(), "synthetic source")
	selected := filepath.Join(root, "Site Files")
	files := map[string]string{
		"index.php":           "<?php echo 'synthetic';\n",
		"wp-config.php":       "<?php define('DB_NAME', 'synthetic');\n",
		"uploads/2026/a.jpg":  "\xff\xd8\xff synthetic jpeg bytes",
		"uploads/2026/b.txt":  strings.Repeat("synthetic text ", 1000),
		"cache/.gitkeep":      "",
		"deep/er/est/end.log": "end\n",
	}
	for name, contents := range files {
		path := filepath.Join(selected, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(selected, "empty"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("uploads/2026", filepath.Join(selected, "current")); err != nil {
		t.Fatal(err)
	}
	stamp := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	_ = filepath.Walk(selected, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode()&os.ModeSymlink == 0 {
			_ = os.Chtimes(path, stamp, stamp)
		}
		return nil
	})

	request.Operation = "backup"
	request.Root = root
	result := requireComplete(t, runner, request)
	snapshotID := ""
	for _, line := range strings.Split(result.Output, "\n") {
		var summary struct {
			SnapshotID string `json:"snapshot_id"`
		}
		if json.Unmarshal([]byte(line), &summary) == nil && summary.SnapshotID != "" {
			snapshotID = summary.SnapshotID
		}
	}

	scratch := filepath.Join(t.TempDir(), "scratch")
	if err := os.MkdirAll(scratch, 0o700); err != nil {
		t.Fatal(err)
	}

	// The fallback would produce the same archive, so prove the restore itself works and cleans up.
	restored, cleanup, err := runner.restoreExport(context.Background(), ExportRequest{
		Version: 1, Connection: request.Connection, Password: request.Password, Snapshot: snapshotID,
		Kind: "directory", Path: selected, ArchiveEntryName: filepath.Base(selected), TimeoutSeconds: 300, Scratch: scratch,
	})
	if err != nil {
		t.Fatalf("scratch restore: %v", err)
	}
	if contents, err := os.ReadFile(filepath.Join(restored, "Site Files", "wp-config.php")); err != nil || string(contents) != files["wp-config.php"] {
		t.Fatalf("restored file: %q %v", contents, err)
	}
	cleanup()
	if _, err := os.Stat(restored); !os.IsNotExist(err) {
		t.Fatalf("scratch restore was not removed: %v", err)
	}

	for _, selection := range []struct {
		kind, path, entry string
	}{
		{"directory", selected, filepath.Base(selected)},
		{"directory", "/", "backup"},
		{"file", filepath.Join(selected, "wp-config.php"), "wp-config.php"},
	} {
		exportRequest := ExportRequest{
			Version: 1, Connection: request.Connection, Password: request.Password, Snapshot: snapshotID,
			Kind: selection.kind, Path: selection.path, ArchiveEntryName: selection.entry, TimeoutSeconds: 300,
		}
		streamed, streamedStats := exportEntries(t, runner, exportRequest)

		exportRequest.Scratch = scratch
		restored, restoredStats := exportEntries(t, runner, exportRequest)

		if streamedStats.Scratch || !restoredStats.Scratch || restoredStats.Restore <= 0 {
			t.Fatalf("%s %s: streamed %+v, restored %+v", selection.kind, selection.path, streamedStats, restoredStats)
		}

		if len(streamed) == 0 || !reflect.DeepEqual(entryNames(streamed), entryNames(restored)) {
			t.Fatalf("%s %s: entry names differ\nstreamed: %v\nrestored: %v", selection.kind, selection.path, entryNames(streamed), entryNames(restored))
		}
		for name, want := range streamed {
			got := restored[name]
			if got.Contents != want.Contents {
				t.Fatalf("%s: entry %s differs: streamed %+v restored %+v", selection.path, name, want, got)
			}
			// The streamed single-file archive uses a bare header; the restored one keeps the
			// file's real mode and time, so only folder exports compare metadata.
			if selection.kind != "directory" {
				continue
			}
			if got.Mode != want.Mode {
				t.Fatalf("%s: entry %s mode %v, want %v", selection.path, name, got.Mode, want.Mode)
			}
			if want.Mode&os.ModeSymlink == 0 && got.Modified != want.Modified {
				t.Fatalf("%s: entry %s modified %d, want %d", selection.path, name, got.Modified, want.Modified)
			}
		}
	}

	runs, _ := filepath.Glob(filepath.Join(scratch, "work", "run-*"))
	if len(runs) != 0 {
		t.Fatalf("scratch restores were left behind: %v", runs)
	}
}

func TestScratchExportFallsBackToStreamingWhenTheScratchDirectoryIsUnusable(t *testing.T) {
	runner := testRunner(t)
	request := testRequest(t)
	requireComplete(t, runner, request)

	root := filepath.Join(t.TempDir(), "synthetic source")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "notes.txt"), []byte("synthetic notes\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	request.Operation = "backup"
	request.Root = root
	result := requireComplete(t, runner, request)
	snapshotID := ""
	for _, line := range strings.Split(result.Output, "\n") {
		var summary struct {
			SnapshotID string `json:"snapshot_id"`
		}
		if json.Unmarshal([]byte(line), &summary) == nil && summary.SnapshotID != "" {
			snapshotID = summary.SnapshotID
		}
	}

	entries, stats := exportEntries(t, runner, ExportRequest{
		Version: 1, Connection: request.Connection, Password: request.Password, Snapshot: snapshotID,
		Kind: "file", Path: filepath.Join(root, "notes.txt"), ArchiveEntryName: "notes.txt", TimeoutSeconds: 300,
		Scratch: filepath.Join(t.TempDir(), "missing"),
	})

	if entries["notes.txt"].Contents != "synthetic notes\n" || stats.Scratch {
		t.Fatalf("fallback archive: %+v (%+v)", entries, stats)
	}
}

func exportEntries(t *testing.T, runner Runner, request ExportRequest) (map[string]archivedEntry, ExportStats) {
	t.Helper()
	var archive bytes.Buffer
	stats, err := runner.StreamExport(context.Background(), request, &archive)
	if err != nil {
		t.Fatalf("export %s %s (scratch %q): %v", request.Kind, request.Path, request.Scratch, err)
	}
	reader, err := zip.NewReader(bytes.NewReader(archive.Bytes()), int64(archive.Len()))
	if err != nil {
		t.Fatal(err)
	}

	entries := map[string]archivedEntry{}
	for _, file := range reader.File {
		opened, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		contents, _ := io.ReadAll(opened)
		_ = opened.Close()
		entries[file.Name] = archivedEntry{Mode: file.Mode(), Contents: string(contents), Modified: file.Modified.Unix()}
	}

	return entries, stats
}

func entryNames(entries map[string]archivedEntry) []string {
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}
