package restic

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSnapshotWalkRequestBoundaries(t *testing.T) {
	request := testRequest(t)
	request.Operation = "walk"
	request.Snapshot = strings.Repeat("f", 64)
	request.MaxNodes = 1_000_000

	args, _, err := request.arguments("password", "", "cache", true)
	want := []string{"find", "--json", "--snapshot", request.Snapshot, "*"}
	if err != nil || !reflect.DeepEqual(args[len(args)-len(want):], want) {
		t.Fatalf("walk arguments: %v %v", args, err)
	}

	for _, invalid := range []Request{
		{Snapshot: "short", MaxNodes: 10},
		{Snapshot: request.Snapshot, MaxNodes: 0},
		{Snapshot: request.Snapshot, MaxNodes: maxWalkNodes + 1},
	} {
		candidate := request
		candidate.Snapshot = invalid.Snapshot
		candidate.MaxNodes = invalid.MaxNodes
		if _, _, err := candidate.arguments("password", "", "cache", true); err == nil {
			t.Fatalf("accepted walk %+v", invalid)
		}
	}
}

func TestSnapshotWalkStreamsCompactNodes(t *testing.T) {
	var stream bytes.Buffer
	result := runWalkFixture(t, "walk", 10, &stream)

	if result.Outcome != "complete" || result.ExitCode != 0 || result.Truncated || result.Output != "" {
		t.Fatalf("walk result: %+v", result)
	}

	nodes := decodeWalkLines(t, stream.String())
	if len(nodes) != 3 {
		t.Fatalf("walk nodes: %+v", nodes)
	}
	if nodes[2] != (WalkNode{Path: "/srv/site/current", Type: "symlink", Mode: 134218221, ModTime: "2026-10-06T10:00:00Z", UID: 33, GID: 33, User: "www-data", Group: "www-data", LinkTarget: "releases/[REDACTED]"}) {
		t.Fatalf("symlink node: %+v", nodes[2])
	}
	if strings.Contains(stream.String(), "inode") || strings.Contains(stream.String(), "synthetic-secret") {
		t.Fatalf("walk forwarded unneeded or secret metadata: %s", stream.String())
	}
}

func TestSnapshotWalkStopsAtItsNodeBudget(t *testing.T) {
	var stream bytes.Buffer
	result := runWalkFixture(t, "walk", 2, &stream)

	if result.Outcome != "partial" || !result.Truncated || result.Diagnostic != "snapshot walk stopped at its node budget" {
		t.Fatalf("budget result: %+v", result)
	}
	if nodes := decodeWalkLines(t, stream.String()); len(nodes) != 2 {
		t.Fatalf("budget nodes: %+v", nodes)
	}
}

func TestSnapshotWalkRejectsInvalidOutput(t *testing.T) {
	var stream bytes.Buffer
	result := runWalkFixture(t, "walk-invalid", 10, &stream)

	if result.Outcome != "failed" || result.Diagnostic != "restic walk output was invalid" {
		t.Fatalf("invalid result: %+v", result)
	}
}

func TestSnapshotWalkReportsATimeoutAsCancelled(t *testing.T) {
	var stream bytes.Buffer
	ctx, cancel := context.WithTimeout(WithWalkStream(context.Background(), &stream), 300*time.Millisecond)
	defer cancel()
	command := exec.Command(os.Args[0], "-test.run=^TestProcessFixture$")
	command.Env = append(os.Environ(), "BACKUPCHIEF_PROCESS_FIXTURE=walk-stall")

	result := runProcess(ctx, command, Request{Operation: "walk", Password: "synthetic-secret", MaxNodes: 10})

	if result.Outcome != "cancelled" {
		t.Fatalf("timeout result: %+v", result)
	}
	if nodes := decodeWalkLines(t, stream.String()); len(nodes) != 1 {
		t.Fatalf("nodes before timeout: %+v", nodes)
	}
}

func TestSnapshotWalkRequiresAStream(t *testing.T) {
	command := exec.Command(os.Args[0], "-test.run=^TestProcessFixture$")
	command.Env = append(os.Environ(), "BACKUPCHIEF_PROCESS_FIXTURE=walk")

	result := runProcess(context.Background(), command, Request{Operation: "walk", MaxNodes: 10})

	if result.Outcome != "failed" || result.Diagnostic != "snapshot walk requires an output stream" {
		t.Fatalf("streamless walk: %+v", result)
	}
}

func TestSnapshotWalkListsARealSnapshot(t *testing.T) {
	runner := testRunner(t)
	request := testRequest(t)
	requireComplete(t, runner, request)

	root := filepath.Join(t.TempDir(), "synthetic walk")
	for name, contents := range map[string]string{
		"visible.txt":           "visible synthetic file",
		"nested/child.txt":      "nested synthetic file",
		"nested/deeper/end.txt": "deep synthetic file",
		".hidden":               "hidden synthetic file",
	} {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("nested", filepath.Join(root, "current")); err != nil {
		t.Fatal(err)
	}

	request.Operation = "backup"
	request.Root = root
	result := requireComplete(t, runner, request)

	var snapshotID string
	for _, line := range strings.Split(result.Output, "\n") {
		var summary struct {
			MessageType string `json:"message_type"`
			SnapshotID  string `json:"snapshot_id"`
		}
		if json.Unmarshal([]byte(line), &summary) == nil && summary.MessageType == "summary" {
			snapshotID = summary.SnapshotID
		}
	}

	var stream bytes.Buffer
	request.Operation = "walk"
	request.Snapshot = snapshotID
	request.MaxNodes = 1000
	result = runner.Run(WithWalkStream(context.Background(), &stream), request)
	if result.Outcome != "complete" || result.ExitCode != 0 {
		t.Fatalf("walk: %+v", result)
	}

	nodes := map[string]WalkNode{}
	order := []string{}
	for _, node := range decodeWalkLines(t, stream.String()) {
		nodes[node.Path] = node
		order = append(order, node.Path)
	}

	for _, relative := range []string{".hidden", "visible.txt", "nested", "nested/child.txt", "nested/deeper/end.txt", "current"} {
		if _, exists := nodes[filepath.Join(root, relative)]; !exists {
			t.Fatalf("walk missed %s: %v", relative, order)
		}
	}
	if current := nodes[filepath.Join(root, "current")]; current.Type != "symlink" || current.LinkTarget != "nested" {
		t.Fatalf("symlink target missing from walk: %+v", current)
	}
	if visible := nodes[filepath.Join(root, "visible.txt")]; visible.Type != "file" || visible.Size != uint64(len("visible synthetic file")) {
		t.Fatalf("file metadata missing from walk: %+v", visible)
	}

	// Depth-first order lets the control plane close each folder as soon as the walk leaves it.
	nested := indexOf(order, filepath.Join(root, "nested"))
	deeper := indexOf(order, filepath.Join(root, "nested/deeper/end.txt"))
	visible := indexOf(order, filepath.Join(root, "visible.txt"))
	if nested >= deeper || deeper >= visible {
		t.Fatalf("walk was not depth-first: %v", order)
	}
}

func runWalkFixture(t *testing.T, mode string, budget int, stream *bytes.Buffer) Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(WithWalkStream(context.Background(), stream), 10*time.Second)
	defer cancel()
	command := exec.Command(os.Args[0], "-test.run=^TestProcessFixture$")
	command.Env = append(os.Environ(), "BACKUPCHIEF_PROCESS_FIXTURE="+mode)

	return runProcess(ctx, command, Request{Operation: "walk", Password: "synthetic-secret", MaxNodes: budget})
}

func decodeWalkLines(t *testing.T, output string) []WalkNode {
	t.Helper()
	nodes := []WalkNode{}

	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if line == "" {
			continue
		}

		var envelope struct {
			Node WalkNode `json:"node"`
		}
		decoder := json.NewDecoder(strings.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&envelope); err != nil {
			t.Fatalf("walk line %q: %v", line, err)
		}
		nodes = append(nodes, envelope.Node)
	}

	return nodes
}

func indexOf(values []string, value string) int {
	for index, candidate := range values {
		if candidate == value {
			return index
		}
	}

	return -1
}

func walkFixture() {
	switch os.Getenv("BACKUPCHIEF_PROCESS_FIXTURE") {
	case "walk":
		_, _ = os.Stdout.WriteString(`[{"matches":[` +
			`{"path":"/srv/site","type":"dir","mode":2147484141,"mtime":"2026-10-06T10:00:00Z","uid":33,"gid":33,"user":"www-data","group":"www-data","inode":1},` +
			`{"path":"/srv/site/index.php","type":"file","size":42,"mode":420,"mtime":"2026-10-06T10:00:00Z","uid":33,"gid":33,"user":"www-data","group":"www-data","inode":2,"links":1},` +
			`{"path":"/srv/site/current","type":"symlink","mode":134218221,"mtime":"2026-10-06T10:00:00Z","uid":33,"gid":33,"user":"www-data","group":"www-data","inode":3,"linktarget":"releases/synthetic-secret"}` +
			`],"hits":3,"snapshot":"` + strings.Repeat("f", 64) + `"}]`)
		os.Exit(0)
	case "walk-invalid":
		_, _ = os.Stdout.WriteString(`{"not":"a walk"}`)
		os.Exit(0)
	case "walk-stall":
		_, _ = os.Stdout.WriteString(`[{"matches":[{"path":"/srv/site","type":"dir","mode":2147484141,"uid":0,"gid":0},`)
		time.Sleep(time.Minute)
		os.Exit(0)
	}
}
