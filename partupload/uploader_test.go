package partupload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testPartBytes = minPartBytes

type partServer struct {
	*httptest.Server
	mu         sync.Mutex
	parts      map[int][]byte
	active     atomic.Int32
	peak       atomic.Int32
	failures   map[int]int
	status     int
	contentLen map[int]int64
}

func newPartServer(t *testing.T) *partServer {
	t.Helper()
	server := &partServer{parts: map[int][]byte{}, failures: map[int]int{}, contentLen: map[int]int64{}, status: http.StatusInternalServerError}
	server.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		active := server.active.Add(1)
		defer server.active.Add(-1)
		for peak := server.peak.Load(); active > peak && !server.peak.CompareAndSwap(peak, active); peak = server.peak.Load() {
		}
		time.Sleep(20 * time.Millisecond)

		part, _ := strconv.Atoi(r.URL.Query().Get("part"))
		body, _ := io.ReadAll(r.Body)

		server.mu.Lock()
		defer server.mu.Unlock()
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if server.failures[part] > 0 {
			server.failures[part]--
			w.WriteHeader(server.status)
			return
		}
		server.parts[part] = body
		server.contentLen[part] = r.ContentLength
		w.Header().Set("ETag", fmt.Sprintf("\"etag-%d\"", part))
	}))
	t.Cleanup(server.Close)

	return server
}

func (server *partServer) target(parts int, maximum int64, concurrency int) Target {
	urls := make([]string, parts)
	for index := range urls {
		urls[index] = fmt.Sprintf("%s/object?part=%d", server.URL, index+1)
	}

	return Target{PartBytes: testPartBytes, Concurrency: concurrency, MaximumBytes: maximum, PartURLs: urls}
}

func archiveBytes(size int) []byte {
	data := make([]byte, size)
	copy(data, "PK\x03\x04")
	for index := 4; index < size; index++ {
		data[index] = byte(index % 251)
	}

	return data
}

func writeInChunks(t *testing.T, writer io.Writer, data []byte, chunk int) error {
	t.Helper()
	for offset := 0; offset < len(data); offset += chunk {
		if _, err := writer.Write(data[offset:min(offset+chunk, len(data))]); err != nil {
			return err
		}
	}

	return nil
}

func TestUploaderUploadsFixedSizePartsInParallelAndReportsTheirETags(t *testing.T) {
	server := newPartServer(t)
	data := archiveBytes(5*testPartBytes + 1234)
	uploader := New(context.Background(), server.Client(), server.target(8, 8*testPartBytes, 3))

	if err := writeInChunks(t, uploader, data, 100_000); err != nil {
		t.Fatal(err)
	}
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}

	parts := uploader.Parts()
	if len(parts) != 6 {
		t.Fatalf("uploaded %d parts, want 6", len(parts))
	}
	var joined []byte
	for index, part := range parts {
		if part.Number != index+1 || part.ETag != fmt.Sprintf("\"etag-%d\"", index+1) {
			t.Fatalf("part %d: %+v", index, part)
		}
		if index < 5 && server.contentLen[part.Number] != testPartBytes {
			t.Fatalf("part %d was %d bytes, want the fixed part size", part.Number, server.contentLen[part.Number])
		}
		joined = append(joined, server.parts[part.Number]...)
	}

	digest := sha256.Sum256(data)
	if !bytes.Equal(joined, data) || uploader.SHA256() != hex.EncodeToString(digest[:]) {
		t.Fatal("uploaded parts do not reassemble the archive")
	}
	if uploader.Written() != int64(len(data)) || uploader.Uploaded() != int64(len(data)) {
		t.Fatalf("written %d, uploaded %d, want %d", uploader.Written(), uploader.Uploaded(), len(data))
	}
	if peak := server.peak.Load(); peak < 2 || peak > 3 {
		t.Fatalf("peak concurrent part uploads %d, want 2..3", peak)
	}
}

func TestUploaderRetriesTransientPartFailures(t *testing.T) {
	server := newPartServer(t)
	server.failures[1] = 2
	uploader := New(context.Background(), server.Client(), server.target(1, testPartBytes, 1))

	if _, err := uploader.Write(archiveBytes(1000)); err != nil {
		t.Fatal(err)
	}
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	if len(uploader.Parts()) != 1 || uploader.Uploaded() != 1000 {
		t.Fatalf("parts %+v, uploaded %d", uploader.Parts(), uploader.Uploaded())
	}
}

func TestUploaderFailsWithACodeTheControlPlaneReports(t *testing.T) {
	server := newPartServer(t)

	cases := map[string]func(*Uploader) error{
		"archive_too_large": func(uploader *Uploader) error {
			return writeInChunks(t, uploader, archiveBytes(testPartBytes+1), 64<<10)
		},
		"invalid_archive": func(uploader *Uploader) error {
			_, err := uploader.Write([]byte("<html>not a zip</html>"))
			return err
		},
		"empty_archive": func(*Uploader) error {
			return nil
		},
		"artifact_storage_failed": func(uploader *Uploader) error {
			server.mu.Lock()
			server.failures[1], server.status = 1, http.StatusForbidden
			server.mu.Unlock()
			_, err := uploader.Write(archiveBytes(1000))
			return err
		},
	}

	for code, write := range cases {
		uploader := New(context.Background(), server.Client(), server.target(1, testPartBytes, 1))
		err := errors.Join(write(uploader), uploader.Close())

		var failure Failure
		if !errors.As(err, &failure) || failure.Code != code {
			t.Fatalf("%s: got %v", code, err)
		}
	}
}

func TestTargetAcceptsOnlyOneHTTPSHostCoveringTheLimit(t *testing.T) {
	valid := Target{PartBytes: testPartBytes, Concurrency: 4, MaximumBytes: 2 * testPartBytes, PartURLs: []string{
		"https://objects.example.test/bucket/key?partNumber=1",
		"https://objects.example.test/bucket/key?partNumber=2",
	}}
	if host, err := valid.Host(); err != nil || host != "objects.example.test" {
		t.Fatalf("valid target: %q %v", host, err)
	}

	invalid := map[string]func(Target) Target{
		"too few parts": func(target Target) Target { target.PartURLs = target.PartURLs[:1]; return target },
		"two hosts": func(target Target) Target {
			target.PartURLs[1] = "https://other.example.test/bucket/key"
			return target
		},
		"plain http": func(target Target) Target {
			target.PartURLs[0] = "http://objects.example.test/bucket/key"
			return target
		},
		"other port": func(target Target) Target {
			target.PartURLs[0] = "https://objects.example.test:8443/bucket/key"
			return target
		},
		"small parts":    func(target Target) Target { target.PartBytes = minPartBytes - 1; return target },
		"no concurrency": func(target Target) Target { target.Concurrency = 0; return target },
		"credentials": func(target Target) Target {
			target.PartURLs[0] = "https://user@objects.example.test/bucket/key"
			return target
		},
		"no maximum size": func(target Target) Target { target.MaximumBytes = 0; return target },
	}
	for name, mutate := range invalid {
		target := valid
		target.PartURLs = append([]string(nil), valid.PartURLs...)
		if _, err := mutate(target).Host(); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
}
