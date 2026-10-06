package restic

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
)

const (
	maxWalkNodes      = 5_000_000
	maxWalkPathBytes  = 16 << 10
	walkFlushInterval = 64 << 10
)

type walkStreamKey struct{}

// WithWalkStream receives one compact JSON line per snapshot node while a walk runs.
func WithWalkStream(ctx context.Context, stream io.Writer) context.Context {
	return context.WithValue(ctx, walkStreamKey{}, stream)
}

func walkStream(ctx context.Context) (io.Writer, bool) {
	stream, ok := ctx.Value(walkStreamKey{}).(io.Writer)

	return stream, ok && stream != nil
}

// WalkNode is the sanitized metadata the control plane needs to browse a recovery point.
type WalkNode struct {
	Path       string `json:"path"`
	Type       string `json:"type"`
	Size       uint64 `json:"size,omitempty"`
	Mode       uint32 `json:"mode"`
	ModTime    string `json:"mtime,omitempty"`
	UID        uint32 `json:"uid"`
	GID        uint32 `json:"gid"`
	User       string `json:"user,omitempty"`
	Group      string `json:"group,omitempty"`
	LinkTarget string `json:"linktarget,omitempty"`
}

// walkOutput parses streamed `restic find --json` output and forwards each match as it arrives,
// so a large snapshot is never buffered in memory.
type walkOutput struct {
	request  Request
	cancel   context.CancelFunc
	pipe     *io.PipeWriter
	done     chan struct{}
	mu       sync.Mutex
	nodes    int
	budget   bool
	invalid  bool
	failed   bool
	finished bool
}

func newWalkOutput(stream io.Writer, request Request, cancel context.CancelFunc) *walkOutput {
	reader, writer := io.Pipe()
	output := &walkOutput{
		request: request,
		cancel:  cancel,
		pipe:    writer,
		done:    make(chan struct{}),
	}

	go func() {
		defer close(output.done)

		buffered := bufio.NewWriterSize(stream, walkFlushInterval)
		err := output.parse(json.NewDecoder(reader), buffered)
		if flushErr := buffered.Flush(); err == nil {
			err = flushErr
		}

		if err != nil {
			output.mu.Lock()
			if !output.budget {
				var streamErr walkStreamError
				if errors.As(err, &streamErr) {
					output.failed = true
				} else {
					output.invalid = true
				}
			}
			output.mu.Unlock()
			output.cancel()
		}

		// Keep draining so Restic never blocks on a full pipe after the walk stops parsing.
		_, _ = io.Copy(io.Discard, reader)
	}()

	return output
}

func (output *walkOutput) Write(data []byte) (int, error) {
	if _, err := output.pipe.Write(data); err != nil {
		return 0, err
	}

	return len(data), nil
}

// Close ends the stream and waits until every parsed node has been forwarded.
func (output *walkOutput) Close() {
	output.mu.Lock()
	if output.finished {
		output.mu.Unlock()
		return
	}
	output.finished = true
	output.mu.Unlock()

	_ = output.pipe.Close()
	<-output.done
}

type walkStreamError struct{ error }

func (output *walkOutput) parse(decoder *json.Decoder, stream io.Writer) error {
	if err := expectDelimiter(decoder, '['); err != nil {
		return err
	}

	for decoder.More() {
		if err := expectDelimiter(decoder, '{'); err != nil {
			return err
		}

		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return err
			}

			key, ok := token.(string)
			if !ok {
				return errors.New("invalid walk object key")
			}

			if key != "matches" {
				var ignored json.RawMessage
				if err := decoder.Decode(&ignored); err != nil {
					return err
				}

				continue
			}

			if err := expectDelimiter(decoder, '['); err != nil {
				return err
			}

			for decoder.More() {
				var node WalkNode
				if err := decoder.Decode(&node); err != nil {
					return err
				}

				if err := output.forward(stream, node); err != nil {
					return err
				}
			}

			if err := expectDelimiter(decoder, ']'); err != nil {
				return err
			}
		}

		if err := expectDelimiter(decoder, '}'); err != nil {
			return err
		}
	}

	return expectDelimiter(decoder, ']')
}

func (output *walkOutput) forward(stream io.Writer, node WalkNode) error {
	if node.Path == "" || len(node.Path) > maxWalkPathBytes || node.Path[0] != '/' {
		return nil
	}
	if len(node.LinkTarget) > maxWalkPathBytes {
		node.LinkTarget = ""
	}

	output.mu.Lock()
	output.nodes++
	exceeded := output.nodes > output.request.MaxNodes
	if exceeded {
		output.budget = true
	}
	output.mu.Unlock()

	if exceeded {
		return errors.New("walk node budget reached")
	}

	node.Path = redact(node.Path, output.request)
	node.LinkTarget = redact(node.LinkTarget, output.request)

	line, err := json.Marshal(struct {
		Node WalkNode `json:"node"`
	}{Node: node})
	if err != nil {
		return err
	}

	if _, err := stream.Write(append(line, '\n')); err != nil {
		return walkStreamError{err}
	}

	return nil
}

func (output *walkOutput) state() (nodes int, budget bool, invalid bool, failed bool) {
	output.mu.Lock()
	defer output.mu.Unlock()

	return output.nodes, output.budget, output.invalid, output.failed
}

func expectDelimiter(decoder *json.Decoder, expected json.Delim) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}

	if delimiter, ok := token.(json.Delim); !ok || delimiter != expected {
		return errors.New("unexpected walk output structure")
	}

	return nil
}
