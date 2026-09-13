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
