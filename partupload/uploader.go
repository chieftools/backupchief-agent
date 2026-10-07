// Package partupload uploads a stream to presigned multipart part URLs that the control plane
// created, so the archive never has to pass through the control plane itself. The control plane
// keeps ownership of the upload: it starts it, completes it with the reported ETags, and aborts it.
package partupload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

const (
	minPartBytes   = 5 << 20
	maxPartBytes   = 5 << 30
	maxParts       = 10_000
	maxConcurrency = 16
	partAttempts   = 3
)

// Target is where the archive goes: one presigned PUT URL per part, in part order.
type Target struct {
	PartBytes    int64    `json:"part_bytes"`
	Concurrency  int      `json:"concurrency"`
	MaximumBytes int64    `json:"maximum_bytes"`
	PartURLs     []string `json:"part_urls"`
}

// Host is the single HTTPS host every part URL points at, after validating the target.
func (target Target) Host() (string, error) {
	if target.PartBytes < minPartBytes || target.PartBytes > maxPartBytes ||
		target.Concurrency < 1 || target.Concurrency > maxConcurrency ||
		target.MaximumBytes < 1 ||
		len(target.PartURLs) < 1 || len(target.PartURLs) > maxParts ||
		int64(len(target.PartURLs)) < (target.MaximumBytes+target.PartBytes-1)/target.PartBytes {
		return "", errors.New("invalid upload target")
	}

	host := ""
	for _, raw := range target.PartURLs {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Hostname() == "" ||
			(parsed.Port() != "" && parsed.Port() != "443") || parsed.Fragment != "" {
			return "", errors.New("invalid upload target")
		}
		if host == "" {
			host = parsed.Hostname()
		} else if parsed.Hostname() != host {
			return "", errors.New("invalid upload target")
		}
	}

	return host, nil
}

// Failure is an upload error with the code the control plane reports to the requester.
type Failure struct {
	Code string
}

func (failure Failure) Error() string {
	return failure.Code
}

// Part is one uploaded part, as the control plane needs it to complete the upload.
type Part struct {
	Number int    `json:"number"`
	ETag   string `json:"etag"`
}

// Uploader is the archive's writer: it fills one part at a time and uploads full parts in the
// background, blocking only while every upload slot is busy. At most concurrency+1 part buffers
// exist at once.
type Uploader struct {
	ctx    context.Context
	cancel context.CancelFunc
	client *http.Client
	target Target

	slots   chan struct{}
	buffers chan []byte
	group   sync.WaitGroup

	mu    sync.Mutex
	err   error
	etags []string

	buffer    []byte
	header    []byte
	hash      hash.Hash
	written   atomic.Int64
	uploaded  atomic.Int64
	waiting   time.Duration
	nextPart  int
	closed    bool
	firstByte atomic.Int64
}

func New(ctx context.Context, client *http.Client, target Target) *Uploader {
	ctx, cancel := context.WithCancel(ctx)

	return &Uploader{
		ctx:      ctx,
		cancel:   cancel,
		client:   client,
		target:   target,
		slots:    make(chan struct{}, target.Concurrency),
		buffers:  make(chan []byte, target.Concurrency+1),
		etags:    make([]string, 0, len(target.PartURLs)),
		hash:     sha256.New(),
		nextPart: 1,
	}
}

func (uploader *Uploader) Write(data []byte) (int, error) {
	if err := uploader.failure(); err != nil {
		return 0, err
	}
	if uploader.written.Load()+int64(len(data)) > uploader.target.MaximumBytes {
		uploader.fail(Failure{Code: "archive_too_large"})
		return 0, uploader.failure()
	}
	if len(data) > 0 && uploader.firstByte.Load() == 0 {
		uploader.firstByte.Store(time.Now().UnixNano())
	}
	if len(uploader.header) < 4 {
		uploader.header = append(uploader.header, data[:min(len(data), 4-len(uploader.header))]...)
		if len(uploader.header) == 4 && !validSignature(uploader.header) {
			uploader.fail(Failure{Code: "invalid_archive"})
			return 0, uploader.failure()
		}
	}

	uploader.hash.Write(data)
	written := 0

	for written < len(data) {
		if uploader.buffer == nil {
			uploader.buffer = uploader.takeBuffer()
		}

		count := copy(uploader.buffer[len(uploader.buffer):cap(uploader.buffer)], data[written:])
		uploader.buffer = uploader.buffer[:len(uploader.buffer)+count]
		written += count
		uploader.written.Add(int64(count))

		if int64(len(uploader.buffer)) == uploader.target.PartBytes {
			if err := uploader.submit(); err != nil {
				return written, err
			}
		}
	}

	return written, nil
}

// Close uploads the final part and waits for every part to finish.
func (uploader *Uploader) Close() error {
	if uploader.closed {
		return uploader.failure()
	}
	uploader.closed = true
	defer uploader.cancel()

	if uploader.failure() == nil {
		switch {
		case uploader.written.Load() == 0:
			uploader.fail(Failure{Code: "empty_archive"})
		case len(uploader.header) < 4:
			uploader.fail(Failure{Code: "invalid_archive"})
		case len(uploader.buffer) > 0:
			_ = uploader.submit()
		}
	}

	started := time.Now()
	uploader.group.Wait()
	uploader.waiting += time.Since(started)

	return uploader.failure()
}

// Abandon stops every upload in flight, for when producing the archive failed.
func (uploader *Uploader) Abandon() {
	uploader.cancel()
	uploader.group.Wait()
}

// Err is the first failure of the upload, if any.
func (uploader *Uploader) Err() error {
	return uploader.failure()
}

func (uploader *Uploader) Parts() []Part {
	uploader.mu.Lock()
	defer uploader.mu.Unlock()

	parts := make([]Part, len(uploader.etags))
	for index, etag := range uploader.etags {
		parts[index] = Part{Number: index + 1, ETag: etag}
	}

	return parts
}

func (uploader *Uploader) SHA256() string {
	return hex.EncodeToString(uploader.hash.Sum(nil))
}

// Written is how many archive bytes were produced so far.
func (uploader *Uploader) Written() int64 {
	return uploader.written.Load()
}

// Uploaded is how many archive bytes object storage received so far, including parts in flight.
func (uploader *Uploader) Uploaded() int64 {
	return uploader.uploaded.Load()
}

// FirstByte is when the archive produced its first byte, or the zero time when it has not yet.
func (uploader *Uploader) FirstByte() time.Time {
	if nanoseconds := uploader.firstByte.Load(); nanoseconds != 0 {
		return time.Unix(0, nanoseconds)
	}

	return time.Time{}
}

// UploadWait is how long the archive was paused because every upload slot was busy, including
// the final wait for the last parts.
func (uploader *Uploader) UploadWait() time.Duration {
	return uploader.waiting
}

func (uploader *Uploader) submit() error {
	started := time.Now()
	select {
	case uploader.slots <- struct{}{}:
	case <-uploader.ctx.Done():
		uploader.fail(Failure{Code: "artifact_storage_failed"})
		return uploader.failure()
	}
	uploader.waiting += time.Since(started)

	if err := uploader.failure(); err != nil {
		<-uploader.slots
		return err
	}

	part := uploader.nextPart
	if part > len(uploader.target.PartURLs) {
		<-uploader.slots
		uploader.fail(Failure{Code: "archive_too_large"})
		return uploader.failure()
	}

	uploader.nextPart++
	body := uploader.buffer
	uploader.buffer = nil
	uploader.mu.Lock()
	uploader.etags = append(uploader.etags, "")
	uploader.mu.Unlock()

	uploader.group.Add(1)
	go func() {
		defer uploader.group.Done()
		defer func() {
			uploader.buffers <- body[:0]
			<-uploader.slots
		}()

		etag, err := uploader.uploadPart(uploader.target.PartURLs[part-1], body)
		if err != nil {
			uploader.fail(Failure{Code: "artifact_storage_failed"})
			return
		}

		uploader.mu.Lock()
		uploader.etags[part-1] = etag
		uploader.mu.Unlock()
	}()

	return nil
}

func (uploader *Uploader) uploadPart(partURL string, body []byte) (string, error) {
	var lastErr error

	for attempt := 1; attempt <= partAttempts; attempt++ {
		if attempt > 1 {
			select {
			case <-time.After(time.Duration(attempt-1) * time.Second):
			case <-uploader.ctx.Done():
				return "", uploader.ctx.Err()
			}
		}

		reader := &countingReader{reader: bytes.NewReader(body), counter: &uploader.uploaded}
		etag, retry, err := uploader.put(partURL, reader, int64(len(body)))
		if err == nil {
			return etag, nil
		}

		uploader.uploaded.Add(-reader.read)
		lastErr = err
		if !retry || uploader.ctx.Err() != nil {
			break
		}
	}

	return "", lastErr
}

func (uploader *Uploader) put(partURL string, body io.Reader, length int64) (string, bool, error) {
	request, err := http.NewRequestWithContext(uploader.ctx, http.MethodPut, partURL, body)
	if err != nil {
		return "", false, err
	}
	request.ContentLength = length

	response, err := uploader.client.Do(request)
	if err != nil {
		return "", true, err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))

	if response.StatusCode != http.StatusOK {
		retry := response.StatusCode >= 500 || response.StatusCode == http.StatusTooManyRequests || response.StatusCode == http.StatusRequestTimeout

		return "", retry, fmt.Errorf("part upload returned HTTP %d", response.StatusCode)
	}

	etag := response.Header.Get("ETag")
	if etag == "" || len(etag) > 1024 {
		return "", false, errors.New("part upload returned no ETag")
	}

	return etag, false, nil
}

func (uploader *Uploader) takeBuffer() []byte {
	select {
	case buffer := <-uploader.buffers:
		return buffer
	default:
		return make([]byte, 0, uploader.target.PartBytes)
	}
}

func (uploader *Uploader) fail(err error) {
	uploader.mu.Lock()
	if uploader.err == nil {
		uploader.err = err
	}
	uploader.mu.Unlock()
	uploader.cancel()
}

func (uploader *Uploader) failure() error {
	uploader.mu.Lock()
	defer uploader.mu.Unlock()

	return uploader.err
}

func validSignature(header []byte) bool {
	return bytes.Equal(header, []byte("PK\x03\x04")) || bytes.Equal(header, []byte("PK\x05\x06"))
}

type countingReader struct {
	reader  io.Reader
	counter *atomic.Int64
	read    int64
}

func (reader *countingReader) Read(buffer []byte) (int, error) {
	count, err := reader.reader.Read(buffer)
	reader.read += int64(count)
	reader.counter.Add(int64(count))

	return count, err
}
