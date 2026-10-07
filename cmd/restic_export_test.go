package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/chieftools/backupchief-agent/partupload"
	"github.com/chieftools/backupchief-agent/restic"
)

func TestResticExportRejectsUnknownFields(t *testing.T) {
	command := newResticExportCommand()
	command.SetArgs([]string{"--mode", "inspect"})
	command.SetIn(bytes.NewBufferString("{\"version\":1,\"shell\":\"synthetic\"}\n"))

	if err := command.Execute(); err == nil {
		t.Fatal("accepted unknown export request field")
	}
}

func TestResticExportRequiresAnExplicitMode(t *testing.T) {
	command := newResticExportCommand()
	command.SetIn(bytes.NewBufferString("{}\n"))

	if err := command.Execute(); err == nil {
		t.Fatal("accepted missing export mode")
	}
}

func TestResticExportUploadRequiresAnUploadTarget(t *testing.T) {
	command := newResticExportCommand()
	command.SetArgs([]string{"--mode", "upload"})
	command.SetIn(bytes.NewBufferString("{\"version\":1}\n"))

	if err := command.Execute(); err == nil {
		t.Fatal("accepted an upload without a target")
	}
}

func TestUploadExportReportsProgressAndTheUploadedParts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("ETag", "\"synthetic-"+r.URL.Query().Get("part")+"\"")
	}))
	defer server.Close()

	var output bytes.Buffer
	produce := func(writer io.Writer) (restic.ExportStats, error) {
		time.Sleep(30 * time.Millisecond)
		_, err := writer.Write([]byte("PK\x03\x04synthetic-zip"))

		return restic.ExportStats{Scratch: true, Restore: 20 * time.Millisecond}, err
	}
	target := partupload.Target{PartBytes: 5 << 20, Concurrency: 2, MaximumBytes: 5 << 20, PartURLs: []string{server.URL + "/object?part=1"}}

	if err := uploadExport(context.Background(), produce, true, target, server.Client(), &exportReporter{output: &output}, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	var first exportProgress
	var result exportResult
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil || first.Type != "progress" || first.Stage != "restoring" {
		t.Fatalf("first line %q", lines[0])
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &result); err != nil || result.Type != "result" {
		t.Fatalf("last line %q", lines[len(lines)-1])
	}
	if result.Bytes != 17 || len(result.Parts) != 1 || result.Parts[0].ETag != "\"synthetic-1\"" || len(result.SHA256) != 64 {
		t.Fatalf("result %+v", result)
	}
	if result.Timings["scratch"] != 1 || result.Timings["parts"] != 1 || result.Timings["part_bytes"] != 5<<20 || result.Timings["concurrency"] != 2 {
		t.Fatalf("timings %+v", result.Timings)
	}
}

func TestUploadExportReportsWhyTheArchiveFailed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("ETag", "\"synthetic\"")
	}))
	defer server.Close()
	target := partupload.Target{PartBytes: 5 << 20, Concurrency: 1, MaximumBytes: 64, PartURLs: []string{server.URL + "/object?part=1"}}

	cases := map[string]func(io.Writer) (restic.ExportStats, error){
		"source_unavailable": func(writer io.Writer) (restic.ExportStats, error) {
			_, _ = writer.Write([]byte("PK\x03\x04"))
			return restic.ExportStats{}, errors.New("snapshot archive failed")
		},
		// The archive writer fails first, so its reason wins over the producer's generic error.
		"archive_too_large": func(writer io.Writer) (restic.ExportStats, error) {
			_, _ = writer.Write([]byte("PK\x03\x04" + strings.Repeat("x", 64)))
			return restic.ExportStats{}, errors.New("snapshot archive failed")
		},
	}

	for code, produce := range cases {
		var output bytes.Buffer
		err := uploadExport(context.Background(), produce, false, target, server.Client(), &exportReporter{output: &output}, time.Hour)

		lines := strings.Split(strings.TrimSpace(output.String()), "\n")
		var failure exportFailure
		if err == nil || json.Unmarshal([]byte(lines[len(lines)-1]), &failure) != nil || failure.Type != "failure" || failure.Code != code {
			t.Fatalf("%s: err %v, output %q", code, err, output.String())
		}
	}
}
