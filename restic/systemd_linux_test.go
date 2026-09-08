package restic

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type serviceFixture struct {
	Roots []string
	State string
	Phase string
	S3    *Connection `json:"s3,omitempty"`
}

func TestSystemdQualification(t *testing.T) {
	if os.Getenv("BACKUPCHIEF_SYSTEMD_TEST") != "1" {
		t.Skip("requires an explicitly authorized disposable systemd host")
	}
	if os.Geteuid() != 0 {
		t.Fatal("qualification setup requires root")
	}
	if exec.Command("systemctl", "is-active", "--quiet", "backupchief").Run() == nil {
		t.Fatal("refusing to replace an active service")
	}

	root, err := os.MkdirTemp("/opt", "backupchief-qualification-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	name := filepath.Base(root)
	fixture := serviceFixture{
		State: filepath.Join("/var/lib/backupchief", name),
		Phase: "exercise",
	}
	defer os.RemoveAll(fixture.State)

	if filename := os.Getenv("RESTIC_S3_CONFIG"); filename != "" {
		data, err := os.ReadFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		var connection Connection
		if err := json.Unmarshal(data, &connection); err != nil {
			t.Fatal(err)
		}

		connection.Prefix += "/linux-" + name
		fixture.S3 = &connection

		receipt, _ := json.Marshal(map[string]string{"prefix": connection.Prefix})
		if err := os.WriteFile("/tmp/backupchief-qualification-s3-receipt.json", receipt, 0600); err != nil {
			t.Fatal(err)
		}
	}

	protectedParents := []string{"/root", "/home", "/tmp", "/var/tmp", "/dev/shm"}

	for _, parent := range protectedParents {
		path := filepath.Join(parent, name)
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(path)

		protectedFile := filepath.Join(path, "protected.txt")
		if err := os.WriteFile(protectedFile, []byte("Synthetic protected contents\n"), 0000); err != nil {
			t.Fatal(err)
		}

		worldWritableDirectory := filepath.Join(path, "world-writable")
		if err := os.Mkdir(worldWritableDirectory, 0777); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(worldWritableDirectory, 0777); err != nil {
			t.Fatal(err)
		}

		fixture.Roots = append(fixture.Roots, path)
	}

	bind := filepath.Join(root, "bind")
	if err := os.Mkdir(bind, 0700); err != nil {
		t.Fatal(err)
	}
	runSystem(t, "mount", "--bind", fixture.Roots[0], bind)
	defer func() {
		runSystem(t, "umount", bind)
	}()

	fixture.Roots = append(fixture.Roots, bind)

	config := filepath.Join(root, "fixture.json")
	writeFixture := func() {
		data, _ := json.Marshal(fixture)
		if err := os.WriteFile(config, data, 0600); err != nil {
			t.Fatal(err)
		}
	}

	writeFixture()

	dropin := "/run/systemd/system/backupchief.service.d/90-qualification-test.conf"
	if _, err := os.Lstat(dropin); !os.IsNotExist(err) {
		t.Fatal("qualification override already exists")
	}
	if err := os.MkdirAll(filepath.Dir(dropin), 0755); err != nil {
		t.Fatal(err)
	}

	unit := fmt.Sprintf(`[Service]
ExecStart=
ExecStart=%s -test.run=^TestServiceWorker$ -test.v
Environment=BACKUPCHIEF_SERVICE_FIXTURE=%s
Restart=no
`, os.Args[0], config)

	if err := os.WriteFile(dropin, []byte(unit), 0644); err != nil {
		t.Fatal(err)
	}

	defer func() {
		_ = exec.Command("systemctl", "stop", "backupchief").Run()
		_ = os.Remove(dropin)
		_ = exec.Command("systemctl", "daemon-reload").Run()
	}()

	runSystem(t, "systemctl", "daemon-reload")
	_ = exec.Command("systemctl", "reset-failed", "backupchief").Run()
	runSystem(t, "systemctl", "start", "backupchief")
	waitForService(t)

	serviceResult := strings.TrimSpace(
		runSystem(t, "systemctl", "show", "backupchief", "--property=Result", "--value"),
	)
	if serviceResult != "success" {
		t.Fatal("service qualification failed", runSystem(t, "journalctl", "-u", "backupchief", "--no-pager", "-n", "80"))
	}

	t.Log(runSystem(t, "journalctl", "-u", "backupchief", "--no-pager", "-n", "25"))

	if fixture.S3 != nil {
		t.Log("Linux S3 protected backup and stock recovery passed")
		return
	}
	fixture.Phase = "wait"
	writeFixture()

	binary, err := Binary(context.Background(), fixture.State, "")
	if err != nil {
		t.Fatal(err)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()

	manual := exec.Command(
		binary,
		"--repo",
		filepath.Join(fixture.State, "repository"),
		"backup",
		"--stdin",
		"--stdin-filename",
		"manual.txt",
	)
	manual.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + root, "RESTIC_PASSWORD=synthetic-service-password"}
	manual.Stdin = reader

	if err := manual.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = writer.Close()
		_ = manual.Wait()
	}()

	waitFor(t, func() bool {
		entries, _ := os.ReadDir(filepath.Join(fixture.State, "repository", "locks"))
		return len(entries) > 0
	})
	runSystem(t, "systemctl", "start", "backupchief")

	waitFor(t, func() bool {
		_, err := os.Stat(filepath.Join(fixture.State, "waiting"))
		return err == nil
	})

	var running []string

	waitFor(t, func() bool {
		running = nil
		entries, _ := filepath.Glob("/proc/[0-9]*/cmdline")
		for _, path := range entries {
			data, _ := os.ReadFile(path)
			if strings.Contains(string(data), fixture.State) && strings.Contains(string(data), "prune") {
				running = append(running, path)
			}
		}
		return len(running) > 0
	})

	start := time.Now()
	runSystem(t, "systemctl", "stop", "backupchief")
	waitFor(t, func() bool {
		for _, path := range running {
			if _, err := os.Stat(path); err == nil {
				return false
			}
		}
		return true
	})
	if time.Since(start) > 15*time.Second {
		t.Fatal("service shutdown exceeded its grace period")
	}
	fixture.Phase = "restart"
	writeFixture()

	_ = exec.Command("systemctl", "reset-failed", "backupchief").Run()
	runSystem(t, "systemctl", "start", "backupchief")
	waitForService(t)

	serviceResult = strings.TrimSpace(
		runSystem(t, "systemctl", "show", "backupchief", "--property=Result", "--value"),
	)
	if serviceResult != "success" {
		t.Fatal("restart cleanup failed", runSystem(t, "journalctl", "-u", "backupchief", "--no-pager", "-n", "30"))
	}

	t.Log("systemd protected reads, denied writes, independent restore, cancellation and restart cleanup passed")
}

func TestServiceWorker(t *testing.T) {
	config := os.Getenv("BACKUPCHIEF_SERVICE_FIXTURE")
	if config == "" {
		t.Skip("service test worker")
	}
	data, err := os.ReadFile(config)
	if err != nil {
		t.Fatal(err)
	}
	var fixture serviceFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(fixture.State, 0700); err != nil {
		t.Fatal(err)
	}

	request := Request{
		Version:   1,
		Operation: "init",
		Connection: Connection{
			Driver: "local",
			Path:   filepath.Join(fixture.State, "repository"),
		},
		Password:        "synthetic-service-password",
		TimeoutSeconds:  300,
		LockWaitSeconds: 0,
	}

	if fixture.S3 != nil {
		request.Connection = *fixture.S3
	}

	runRequest := func() Result {
		encoded, _ := json.Marshal(request)
		input := filepath.Join(fixture.State, "request.json")

		if err := os.WriteFile(input, encoded, 0600); err != nil {
			t.Fatal(err)
		}

		command := exec.Command(
			"/usr/bin/backupchief",
			"restic",
			"--state",
			fixture.State,
			"--development-local",
			"--request-file",
			input,
		)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("helper: %v %s", err, output)
		}

		var result Result

		if err := json.Unmarshal(output, &result); err != nil {
			t.Fatalf("helper output: %v %s", err, output)
		}
		if result.Outcome != "complete" {
			t.Fatalf("%s: %+v", request.Operation, result)
		}

		return result
	}

	if fixture.Phase == "wait" {
		request.Operation = "prune"
		request.LockWaitSeconds = 30
		_ = os.WriteFile(filepath.Join(fixture.State, "waiting"), nil, 0600)
		runRequest()
		return
	}
	if fixture.Phase == "restart" {
		request.Operation = "key_verify"
		runRequest()
		entries, _ := os.ReadDir(filepath.Join(fixture.State, "work"))
		if len(entries) != 0 {
			t.Fatal("restart retained abandoned work")
		}

		return
	}

	runRequest()

	request.Operation = "key_add"
	request.NewPassword = "synthetic-recovery-password"
	runRequest()

	for _, root := range fixture.Roots {
		protectedContents, err := os.ReadFile(filepath.Join(root, "protected.txt"))
		if err != nil || string(protectedContents) != "Synthetic protected contents\n" {
			t.Fatalf("protected read %s: %v", root, err)
		}

		write := filepath.Join(root, "world-writable", "unauthorized")
		if err := os.WriteFile(write, []byte("must fail"), 0600); err == nil {
			_ = os.Remove(write)
			t.Fatalf("unauthorized write succeeded: %s", root)
		}

		request.Operation = "backup"
		request.Root = root
		runRequest()
		t.Logf("protected backup and denied write: %s", root)
	}

	request.Operation = "snapshots"
	result := runRequest()

	var snapshots []struct {
		ID    string   `json:"id"`
		Paths []string `json:"paths"`
	}
	if err := json.Unmarshal([]byte(result.Output), &snapshots); err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != len(fixture.Roots) {
		t.Fatalf("snapshot count %d", len(snapshots))
	}

	request.Operation = "check"
	runRequest()

	binary, err := Binary(context.Background(), fixture.State, "")
	if err != nil {
		t.Fatal(err)
	}
	for index, password := range []string{"synthetic-service-password", "synthetic-recovery-password"} {
		for _, snapshot := range snapshots {
			target := filepath.Join(fixture.State, fmt.Sprintf("restore-%d-%s", index, snapshot.ID))
			command := exec.Command(
				binary,
				"--repo",
				request.Connection.Path,
				"restore",
				snapshot.ID,
				"--target",
				target,
				"--verify",
			)
			command.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + fixture.State, "RESTIC_PASSWORD=" + password}

			if fixture.S3 != nil {
				connection := fixture.S3
				repository := "s3:" + strings.TrimSuffix(connection.Endpoint, "/") +
					"/" + connection.Bucket +
					"/" + connection.Prefix

				command = exec.Command(
					binary,
					"--repo",
					repository,
					"-o",
					"s3.region="+connection.Region,
					"-o",
					"s3.bucket-lookup=path",
					"restore",
					snapshot.ID,
					"--target",
					target,
					"--verify",
				)
				command.Env = []string{
					"PATH=/usr/bin:/bin",
					"HOME=" + fixture.State,
					"RESTIC_PASSWORD=" + password,
					"AWS_ACCESS_KEY_ID=" + connection.AccessKey,
					"AWS_SECRET_ACCESS_KEY=" + connection.SecretKey,
				}
			}

			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("stock restore: %v %s", err, output)
			}

			path := filepath.Join(target, strings.TrimPrefix(snapshot.Paths[0], "/"), "protected.txt")
			recoveredContents, err := os.ReadFile(path)
			if err != nil || string(recoveredContents) != "Synthetic protected contents\n" {
				t.Fatalf("recovery contents: %v", err)
			}
		}
	}
}

func waitForService(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Minute)

	for time.Now().Before(deadline) {
		state := strings.TrimSpace(runSystem(t, "systemctl", "show", "backupchief", "--property=ActiveState", "--value"))
		if state == "inactive" || state == "failed" {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatal("service exceeded qualification deadline")
}

func runSystem(t *testing.T, name string, args ...string) string {
	t.Helper()
	command := exec.Command(name, args...)
	output, err := command.CombinedOutput()

	if err != nil {
		t.Fatalf("%s: %v %s", name, err, output)
	}
	return string(output)
}
