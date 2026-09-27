package restic

import (
	"context"
	"io"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestProgressOutputParsesChunkedBoundedSanitizedCounters(t *testing.T) {
	var received []Progress
	output := &progressOutput{ctx: WithProgress(context.Background(), func(value Progress) {
		received = append(received, value)
	})}

	chunks := []string{
		`{"message_type":"sta`,
		"tus\",\"bytes_done\":8192,\"files_done\":2,\"current_files\":[\"/private/synthetic-secret\"]}\n",
		"not json\n{\"message_type\":\"summary\"}\n",
		strings.Repeat("x", 128<<10) + "\n",
		"{\"message_type\":\"status\",\"bytes_done\":16384}\n",
	}

	for _, chunk := range chunks {
		if count, err := output.Write([]byte(chunk)); err != nil || count != len(chunk) {
			t.Fatalf("write: %d %v", count, err)
		}
	}

	if len(received) != 2 {
		t.Fatalf("unexpected samples: %#v", received)
	}

	if len(received[0].Counters) != 2 ||
		received[0].Counters["bytes_processed"] != 8192 ||
		received[1].Counters["bytes_processed"] != 16384 {
		t.Fatalf("unexpected counters: %#v", received)
	}

	if len(output.pending) != 0 {
		t.Fatal("unbounded output retained")
	}
}

func TestRunProcessReportsActivityBeforeExitAndRetainsSummary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	observed := make(chan Progress, 8)
	ctx = WithProgress(ctx, func(value Progress) { observed <- value })

	input, release := io.Pipe()
	defer input.Close()
	defer release.Close()

	command := exec.Command("/bin/sh", "-c", `
        printf '%s\n' '{"message_type":"status","bytes_done":4096,"files_done":1}'
        read ignored
        printf '%s\n' '{"message_type":"summary","total_bytes_processed":4096}'
    `)
	command.Stdin = input

	finished := make(chan Result, 1)
	go func() {
		finished <- runProcess(ctx, command, Request{Operation: "backup"})
	}()

	for {
		select {
		case value := <-observed:
			if value.Counters["bytes_processed"] == 4096 {
				_, _ = release.Write([]byte("finish\n"))
				_ = release.Close()

				result := <-finished
				if result.ExitCode != 0 || !strings.Contains(result.Output, `"message_type":"summary"`) {
					t.Fatalf("lost terminal output: %+v", result)
				}

				return
			}

		case result := <-finished:
			t.Fatalf("finished before reporting: %+v", result)

		case <-ctx.Done():
			t.Fatal("no incremental activity before process exit")
		}
	}
}

func TestProgressObserverPreservesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	started := make(chan struct{}, 1)
	ctx = WithProgress(ctx, func(value Progress) {
		if value.Stage == "executing" {
			started <- struct{}{}
		}
	})

	command := exec.Command("/bin/sh", "-c", "exec sleep 30")
	done := make(chan Result, 1)
	go func() {
		done <- runProcess(ctx, command, Request{Operation: "backup"})
	}()

	<-started
	cancel()

	select {
	case result := <-done:
		if result.Outcome != "cancelled" {
			t.Fatalf("unexpected cancellation: %+v", result)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("cancellation did not stop process")
	}
}
