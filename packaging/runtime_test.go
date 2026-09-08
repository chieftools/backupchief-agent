package packaging_test

import (
	"bytes"
	"crypto/sha256"
	"debug/elf"
	"debug/macho"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

type runtimeArtifact struct {
	File   string `json:"file"`
	SHA256 string `json:"sha256"`
	Size   int    `json:"size"`
}

type runtimeManifest struct {
	Schema        int                                   `json:"schema"`
	Version       string                                `json:"version"`
	ResticVersion string                                `json:"restic_version"`
	Platforms     map[string]map[string]runtimeArtifact `json:"platforms"`
}

func TestRuntimeReleaseContents(t *testing.T) {
	directory := os.Getenv("RUNTIME_TEST_DIR")
	if directory == "" {
		t.Skip("set RUNTIME_TEST_DIR after bin/build-runtime.sh to inspect runtime artifacts")
	}
	version := os.Getenv("RELEASE_VERSION")
	if version == "" {
		t.Fatal("RELEASE_VERSION is required with RUNTIME_TEST_DIR")
	}
	resticVersion := strings.TrimSpace(string(read(t, "../restic/VERSION")))

	var manifest runtimeManifest
	decoder := json.NewDecoder(bytes.NewReader(read(t, filepath.Join(directory, "manifest.json"))))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Schema != 1 || manifest.Version != version || manifest.ResticVersion != resticVersion {
		t.Fatalf("unexpected runtime release metadata: %+v", manifest)
	}

	expected := map[string]map[string]string{
		"darwin-arm64": {
			"backupchief": "backupchief-" + version + "-darwin-arm64",
			"restic":      "restic-" + resticVersion + "-darwin-arm64",
		},
		"linux-amd64": {
			"backupchief": "backupchief-" + version + "-linux-amd64",
		},
		"linux-arm64": {
			"backupchief": "backupchief-" + version + "-linux-arm64",
		},
	}
	if len(manifest.Platforms) != len(expected) {
		t.Fatalf("unexpected platforms: %+v", manifest.Platforms)
	}

	for platform, roles := range expected {
		artifacts, ok := manifest.Platforms[platform]
		if !ok || len(artifacts) != len(roles) {
			t.Fatalf("unexpected artifacts for %s: %+v", platform, artifacts)
		}
		for role, file := range roles {
			artifact, ok := artifacts[role]
			if !ok || artifact.File != file {
				t.Fatalf("unexpected %s artifact for %s: %+v", role, platform, artifact)
			}
			path := filepath.Join(directory, file)
			data := read(t, path)
			hash := sha256.Sum256(data)
			if artifact.Size != len(data) || artifact.SHA256 != hex.EncodeToString(hash[:]) {
				t.Fatalf("runtime artifact integrity mismatch: %s", file)
			}
			info, err := os.Stat(path)
			if err != nil || info.Mode().Perm() != 0755 {
				t.Fatalf("runtime artifact mode: %s: %v", file, err)
			}
			verifyRuntimeFormat(t, path, platform)
		}
	}

	if !bytes.Equal(read(t, filepath.Join(directory, "LICENSE")), read(t, "../LICENSE")) {
		t.Fatal("runtime release is missing the Backup Chief license")
	}
	if !bytes.Equal(read(t, filepath.Join(directory, "restic-LICENSE")), read(t, "../restic/LICENSE")) {
		t.Fatal("runtime release is missing the Restic license")
	}
	if !strings.Contains(string(read(t, filepath.Join(directory, "RELEASE_NOTES.md"))), "## ["+version+"]") {
		t.Fatal("runtime release notes do not identify the release")
	}
	for _, file := range []string{"SHA256SUMS", "manifest.json"} {
		if len(read(t, filepath.Join(directory, file))) == 0 {
			t.Fatalf("runtime release file is empty: %s", file)
		}
	}

	currentPlatform := runtime.GOOS + "-" + runtime.GOARCH
	if artifacts, ok := manifest.Platforms[currentPlatform]; ok {
		helper := filepath.Join(directory, artifacts["backupchief"].File)
		if output := run(t, helper, "version"); output != "Backup Chief agent "+version+"\n" {
			t.Fatalf("unexpected runtime helper version: %s", output)
		}
		if resticArtifact, ok := artifacts["restic"]; ok {
			output := run(t, filepath.Join(directory, resticArtifact.File), "version")
			if !strings.HasPrefix(output, "restic "+resticVersion+" ") {
				t.Fatalf("unexpected runtime Restic version: %s", output)
			}
		}
	}
}

func verifyRuntimeFormat(t *testing.T, path, platform string) {
	t.Helper()
	if strings.HasPrefix(platform, "linux-") {
		executable, err := elf.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer executable.Close()
		expectedMachine := elf.EM_X86_64
		if platform == "linux-arm64" {
			expectedMachine = elf.EM_AARCH64
		}
		if executable.Machine != expectedMachine {
			t.Fatalf("ELF architecture: %s", executable.Machine)
		}
		return
	}

	executable, err := macho.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer executable.Close()
	if executable.Cpu != macho.CpuArm64 {
		t.Fatalf("Mach-O architecture: %s", executable.Cpu)
	}
}
