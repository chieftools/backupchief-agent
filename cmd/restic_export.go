package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/chieftools/backupchief-agent/egress"
	"github.com/chieftools/backupchief-agent/partupload"
	"github.com/chieftools/backupchief-agent/restic"
	"github.com/spf13/cobra"
)

// An upload request carries one presigned URL per part, so it is allowed to be larger than the
// other helper requests.
const maxResticExportRequestBytes = 16 << 20

const exportProgressInterval = time.Second

type resticExportRequest struct {
	restic.ExportRequest
	Upload *partupload.Target `json:"upload,omitempty"`
}

type exportProgress struct {
	Type          string `json:"type"`
	Stage         string `json:"stage"`
	BytesPrepared int64  `json:"bytes_prepared"`
	BytesUploaded int64  `json:"bytes_uploaded"`
}

type exportResult struct {
	Type    string             `json:"type"`
	Bytes   int64              `json:"bytes"`
	SHA256  string             `json:"sha256"`
	Parts   []partupload.Part  `json:"parts"`
	Timings map[string]float64 `json:"timings"`
}

type exportFailure struct {
	Type string `json:"type"`
	Code string `json:"code"`
}

func newResticExportCommand() *cobra.Command {
	var state string
	var mode string
	var allowLocal bool

	command := &cobra.Command{
		Use:    "restic-export",
		Short:  "Inspect or upload one bounded snapshot export",
		Args:   cobra.NoArgs,
		Hidden: true,
		RunE: func(command *cobra.Command, _ []string) error {
			return runResticExportCommand(command, state, mode, allowLocal)
		},
	}
	command.Flags().StringVar(&state, "state", "/var/lib/backupchief", "Private execution state directory")
	command.Flags().StringVar(&mode, "mode", "", "Export mode")
	command.Flags().BoolVar(&allowLocal, "development-local", false, "Permit a local repository for development tests")

	return command
}

func runResticExportCommand(command *cobra.Command, state, mode string, allowLocal bool) error {
	if mode != "inspect" && mode != "upload" {
		return errors.New("invalid snapshot export mode")
	}

	signalContext, cancelSignals := signal.NotifyContext(command.Context(), os.Interrupt, syscall.SIGTERM)
	defer cancelSignals()

	reader := bufio.NewReaderSize(io.LimitReader(command.InOrStdin(), maxResticExportRequestBytes), maxResticExportRequestBytes)
	line, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return errors.New("cannot read snapshot export request")
	}
	if len(line) >= maxResticExportRequestBytes {
		return errors.New("snapshot export request exceeded its limit")
	}

	var request resticExportRequest
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&request); err != nil {
		return errors.New("invalid snapshot export request")
	}

	runner := restic.Runner{State: state, AllowLocal: allowLocal}
	if mode == "upload" {
		if request.Upload == nil {
			return errors.New("invalid snapshot export request")
		}

		return runExportUpload(signalContext, runner, request.ExportRequest, *request.Upload, command.OutOrStdout())
	}

	inspection, err := runner.InspectExport(signalContext, request.ExportRequest)
	if err != nil {
		return err
	}

	return json.NewEncoder(command.OutOrStdout()).Encode(inspection)
}

// runExportUpload sends every part through a per-operation egress proxy, so the uploads obey
// the same public-endpoint and DNS-pinning policy as Restic's own storage connections.
func runExportUpload(ctx context.Context, runner restic.Runner, request restic.ExportRequest, target partupload.Target, output io.Writer) error {
	reporter := &exportReporter{output: output}

	host, err := target.Host()
	if err != nil {
		return reporter.fail("artifact_storage_failed")
	}

	proxy, err := egress.Start(ctx, "https://"+host)
	if err != nil {
		return reporter.fail("artifact_storage_failed")
	}
	defer proxy.Close()

	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		return reporter.fail("artifact_storage_failed")
	}

	client := &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyURL(proxyURL),
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 2 * time.Minute,
			IdleConnTimeout:       90 * time.Second,
			MaxIdleConnsPerHost:   target.Concurrency,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	produce := func(writer io.Writer) (restic.ExportStats, error) {
		return runner.StreamExport(ctx, request, writer)
	}

	return uploadExport(ctx, produce, request.Scratch != "", target, client, reporter, exportProgressInterval)
}

// uploadExport produces the archive straight into the part uploader, reporting progress on
// every interval and finishing with one result line, or one failure line with its code.
func uploadExport(
	ctx context.Context,
	produce func(io.Writer) (restic.ExportStats, error),
	restoresFirst bool,
	target partupload.Target,
	client *http.Client,
	reporter *exportReporter,
	interval time.Duration,
) error {
	started := time.Now()
	uploader := partupload.New(ctx, client, target)

	progress := func() {
		stage := "archiving"
		if restoresFirst && uploader.FirstByte().IsZero() {
			stage = "restoring"
		}
		_ = reporter.write(exportProgress{Type: "progress", Stage: stage, BytesPrepared: uploader.Written(), BytesUploaded: uploader.Uploaded()})
	}

	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		progress()
		for {
			select {
			case <-ticker.C:
				progress()
			case <-done:
				return
			}
		}
	}()
	stopProgress := func() {
		close(done)
		<-stopped
	}

	stats, err := produce(uploader)
	produced := time.Now()
	waitWhileProducing := uploader.UploadWait()

	if err != nil {
		uploader.Abandon()
		stopProgress()

		return reporter.fail(failureCode(uploader.Err(), "source_unavailable"))
	}

	if err = uploader.Close(); err != nil {
		stopProgress()

		return reporter.fail(failureCode(err, "artifact_storage_failed"))
	}

	stopProgress()
	progress()

	timings := map[string]float64{
		"seconds":             seconds(time.Since(started)),
		"restore_seconds":     seconds(stats.Restore),
		"archive_seconds":     seconds(produced.Sub(started) - stats.Restore - waitWhileProducing),
		"upload_wait_seconds": seconds(uploader.UploadWait()),
		"first_byte_seconds":  seconds(uploader.FirstByte().Sub(started)),
		"scratch":             0,
		"parts":               float64(len(uploader.Parts())),
		"part_bytes":          float64(target.PartBytes),
		"concurrency":         float64(target.Concurrency),
	}
	if stats.Scratch {
		timings["scratch"] = 1
	}

	return reporter.write(exportResult{
		Type:    "result",
		Bytes:   uploader.Written(),
		SHA256:  uploader.SHA256(),
		Parts:   uploader.Parts(),
		Timings: timings,
	})
}

func failureCode(err error, fallback string) string {
	var failure partupload.Failure
	if errors.As(err, &failure) {
		return failure.Code
	}

	return fallback
}

func seconds(duration time.Duration) float64 {
	return math.Round(max(duration, 0).Seconds()*10) / 10
}

// exportReporter writes one JSON line per message; progress comes from a background ticker.
type exportReporter struct {
	mu     sync.Mutex
	output io.Writer
}

func (reporter *exportReporter) write(message any) error {
	reporter.mu.Lock()
	defer reporter.mu.Unlock()

	return json.NewEncoder(reporter.output).Encode(message)
}

func (reporter *exportReporter) fail(code string) error {
	_ = reporter.write(exportFailure{Type: "failure", Code: code})

	return errors.New("snapshot export failed: " + code)
}
