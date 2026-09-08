package packaging_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestReleaseNotesContainOnlyRequestedChangelogSection(t *testing.T) {
	changelog := filepath.Join(t.TempDir(), "CHANGELOG.md")
	contents := `# Changelog

## [Unreleased]

## [2.4.0]

### Fixed

- Corrected synthetic release behavior.

## [2.3.0]

- Previous release.

[Unreleased]: https://agent.example.invalid/compare/v2.4.0...HEAD
[2.4.0]: https://agent.example.invalid/releases/tag/v2.4.0
`
	if err := os.WriteFile(changelog, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("../bin/changelog-section.sh", "2.4.0")
	command.Env = append(os.Environ(), "CHANGELOG_PATH="+changelog)

	output, err := command.CombinedOutput()

	if err != nil {
		t.Fatalf("extract release notes: %v: %s", err, output)
	}
	if string(output) != "### Fixed\n\n- Corrected synthetic release behavior.\n" {
		t.Fatalf("unexpected release notes: %q", output)
	}
}

func TestReleaseNotesRejectMissingVersion(t *testing.T) {
	changelog := filepath.Join(t.TempDir(), "CHANGELOG.md")
	if err := os.WriteFile(changelog, []byte("# Changelog\n\n## [3.1.0]\n\n- Synthetic release.\n"), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("../bin/changelog-section.sh", "3.2.0")
	command.Env = append(os.Environ(), "CHANGELOG_PATH="+changelog)

	output, err := command.CombinedOutput()

	if err == nil || !strings.Contains(string(output), "Expected exactly one changelog section for ## [3.2.0], found 0") {
		t.Fatalf("unexpected missing-version result: %v: %s", err, output)
	}
}
