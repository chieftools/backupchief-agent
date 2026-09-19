package restic

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExportSelectionBoundaries(t *testing.T) {
	request := ExportRequest{
		Version:          1,
		Kind:             "directory",
		Path:             "/srv/synthetic records",
		ArchiveEntryName: "synthetic records",
	}
	if !validExportSelection(request) {
		t.Fatal("rejected normalized directory selection")
	}

	for _, mutate := range []func(*ExportRequest){
		func(request *ExportRequest) { request.Kind = "device" },
		func(request *ExportRequest) { request.Path = "/srv/../private" },
		func(request *ExportRequest) { request.ArchiveEntryName = "different" },
		func(request *ExportRequest) { request.ArchiveEntryName = "nested/name" },
	} {
		candidate := request
		mutate(&candidate)
		if validExportSelection(candidate) {
			t.Fatalf("accepted invalid selection: %+v", candidate)
		}
	}
}

func TestSnapshotDirectoryExportIncludesSelectedTopLevelFolder(t *testing.T) {
	runner := testRunner(t)
	request := testRequest(t)
	requireComplete(t, runner, request)

	root := filepath.Join(t.TempDir(), "synthetic source")
	selected := filepath.Join(root, "Client Records")
	if err := os.MkdirAll(selected, 0700); err != nil {
		t.Fatal(err)
	}
	contents := []byte("Synthetic quarterly export\n")
	if err := os.WriteFile(filepath.Join(selected, "summary.txt"), contents, 0600); err != nil {
		t.Fatal(err)
	}

	request.Operation = "backup"
	request.Root = root
	result := requireComplete(t, runner, request)

	var summary struct {
		SnapshotID string `json:"snapshot_id"`
	}
	for _, line := range strings.Split(result.Output, "\n") {
		if json.Unmarshal([]byte(line), &summary) == nil && summary.SnapshotID != "" {
			break
		}
	}
	if summary.SnapshotID == "" {
		t.Fatalf("missing snapshot id: %s", result.Output)
	}

	exportRequest := ExportRequest{
		Version:          1,
		Connection:       request.Connection,
		Password:         request.Password,
		Snapshot:         summary.SnapshotID,
		Kind:             "directory",
		Path:             selected,
		ArchiveEntryName: filepath.Base(selected),
		TimeoutSeconds:   300,
		LockWaitSeconds:  0,
	}
	inspection, err := runner.InspectExport(context.Background(), exportRequest)
	if err != nil || inspection.SourceBytes != int64(len(contents)) {
		t.Fatalf("inspection: %+v %v", inspection, err)
	}

	var archive bytes.Buffer
	if err = runner.StreamExport(context.Background(), exportRequest, &archive); err != nil {
		t.Fatal(err)
	}
	reader, err := zip.NewReader(bytes.NewReader(archive.Bytes()), int64(archive.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if len(reader.File) != 1 || reader.File[0].Name != "Client Records/summary.txt" {
		t.Fatalf("archive entries: %+v", reader.File)
	}

	fileRequest := exportRequest
	fileRequest.Kind = "file"
	fileRequest.Path = filepath.Join(selected, "summary.txt")
	fileRequest.ArchiveEntryName = "summary.txt"
	var fileArchive bytes.Buffer
	if err = runner.StreamExport(context.Background(), fileRequest, &fileArchive); err != nil {
		t.Fatal(err)
	}
	fileReader, err := zip.NewReader(bytes.NewReader(fileArchive.Bytes()), int64(fileArchive.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if len(fileReader.File) != 1 || fileReader.File[0].Name != "summary.txt" {
		t.Fatalf("file archive entries: %+v", fileReader.File)
	}
	entry, err := fileReader.File[0].Open()
	if err != nil {
		t.Fatal(err)
	}
	archivedContents, err := io.ReadAll(entry)
	_ = entry.Close()
	if err != nil || !bytes.Equal(archivedContents, contents) {
		t.Fatalf("file archive contents: %q %v", archivedContents, err)
	}
}

func TestCancelledSnapshotExportRemovesItsRepositoryLock(t *testing.T) {
	runner := testRunner(t)
	request := testRequest(t)
	requireComplete(t, runner, request)

	root := filepath.Join(t.TempDir(), "synthetic export source")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "archive.bin"), []byte(strings.Repeat("synthetic export data\n", 4096)), 0o600); err != nil {
		t.Fatal(err)
	}

	request.Operation = "backup"
	request.Root = root
	result := requireComplete(t, runner, request)
	var summary struct {
		MessageType string `json:"message_type"`
		SnapshotID  string `json:"snapshot_id"`
	}
	for _, line := range strings.Split(result.Output, "\n") {
		var candidate struct {
			MessageType string `json:"message_type"`
			SnapshotID  string `json:"snapshot_id"`
		}
		if json.Unmarshal([]byte(line), &candidate) == nil && candidate.MessageType == "summary" {
			summary = candidate
		}
	}
	if summary.SnapshotID == "" {
		t.Fatalf("missing snapshot id: %s", result.Output)
	}

	exportRequest := ExportRequest{
		Version: 1, Connection: request.Connection, Password: request.Password,
		Snapshot: summary.SnapshotID, Kind: "directory", Path: root,
		ArchiveEntryName: filepath.Base(root), TimeoutSeconds: 300, LockWaitSeconds: 0,
	}
	ctx, cancel := context.WithCancel(context.Background())
	releaseOutput := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- runner.StreamExport(ctx, exportRequest, blockingExportWriter{release: releaseOutput})
	}()

	waitFor(t, func() bool {
		entries, _ := os.ReadDir(filepath.Join(request.Connection.RepositoryPath(), "locks"))
		for _, entry := range entries {
			if snapshotPattern.MatchString(entry.Name()) {
				return true
			}
		}
		return false
	})
	cancel()
	time.Sleep(100 * time.Millisecond)
	close(releaseOutput)

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled export completed successfully")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("cancelled export did not stop")
	}
	waitFor(t, func() bool {
		entries, _ := os.ReadDir(filepath.Join(request.Connection.RepositoryPath(), "locks"))
		return len(entries) == 0
	})
}

type blockingExportWriter struct {
	release <-chan struct{}
}

func (writer blockingExportWriter) Write(contents []byte) (int, error) {
	<-writer.release
	return len(contents), nil
}
