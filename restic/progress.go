package restic

import (
	"bytes"
	"context"
	"encoding/json"
)

type Progress struct {
	Stage    string
	Counters map[string]uint64
}

type progressKey struct{}

// WithProgress observes sanitized activity without changing the retained command output.
func WithProgress(ctx context.Context, observer func(Progress)) context.Context {
	return context.WithValue(ctx, progressKey{}, observer)
}

func reportProgress(ctx context.Context, value Progress) {
	if observer, ok := ctx.Value(progressKey{}).(func(Progress)); ok {
		observer(value)
	}
}

type progressOutput struct {
	ctx        context.Context
	pending    []byte
	discarding bool
}

func (output *progressOutput) Write(data []byte) (int, error) {
	length := len(data)

	for len(data) > 0 {
		end := bytes.IndexByte(data, '\n')
		piece := data
		if end >= 0 {
			piece = data[:end]
		}

		if len(output.pending)+len(piece) > 64<<10 {
			output.pending = nil
			output.discarding = true
		}

		if !output.discarding {
			output.pending = append(output.pending, piece...)
		}

		if end < 0 {
			break
		}

		if !output.discarding {
			output.report(output.pending)
		}

		output.pending = nil
		output.discarding = false
		data = data[end+1:]
	}

	return length, nil
}

func (output *progressOutput) report(line []byte) {
	var value struct {
		Type  string `json:"message_type"`
		Bytes uint64 `json:"bytes_done"`
		Files uint64 `json:"files_done"`
	}

	if json.Unmarshal(line, &value) != nil || value.Type != "status" {
		return
	}

	const maximum = 9007199254740991
	reportProgress(output.ctx, Progress{
		Stage: "backing_up",
		Counters: map[string]uint64{
			"bytes_processed": min(value.Bytes, maximum),
			"files_processed": min(value.Files, maximum),
		},
	})
}
