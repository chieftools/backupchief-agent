package packaging_test

import (
	"bytes"
	"debug/elf"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPackageContents(t *testing.T) {
	packageDirectory := os.Getenv("PACKAGE_TEST_DIR")
	if packageDirectory == "" {
		t.Skip("set PACKAGE_TEST_DIR after bin/build-all.sh to inspect real archives")
	}
	releaseVersion := os.Getenv("RELEASE_VERSION")
	for _, target := range []struct {
		format  string
		arch    string
		goarch  string
		machine elf.Machine
	}{
		{format: "deb", arch: "amd64", goarch: "amd64", machine: elf.EM_X86_64},
		{format: "deb", arch: "arm64", goarch: "arm64", machine: elf.EM_AARCH64},
		{format: "rpm", arch: "x86_64", goarch: "amd64", machine: elf.EM_X86_64},
		{format: "rpm", arch: "aarch64", goarch: "arm64", machine: elf.EM_AARCH64},
	} {
		t.Run(target.format+"/"+target.arch, func(t *testing.T) {
			pattern := "backupchief_*_" + target.arch + ".deb"
			if releaseVersion != "" {
				pattern = "backupchief_" + releaseVersion + "_" + target.arch + ".deb"
			}
			if target.format == "rpm" {
				pattern = "backupchief-*." + target.arch + ".rpm"
				if releaseVersion != "" {
					pattern = "backupchief-" + releaseVersion + "-1." + target.arch + ".rpm"
				}
			}
			packages, err := filepath.Glob(filepath.Join(packageDirectory, pattern))
			if err != nil || len(packages) != 1 {
				t.Fatalf("expected one package for %s: %v %v", pattern, packages, err)
			}
			archive := packages[0]
			root := t.TempDir()
			if target.format == "deb" {
				identity := run(t, "dpkg-deb", "-f", archive, "Package", "Architecture", "Maintainer")
				for _, want := range []string{
					"Package: backupchief",
					"Architecture: " + target.arch,
					"Maintainer: Backup Chief Team <hello@chief.app>",
				} {
					if !strings.Contains(identity, want) {
						t.Fatalf("unexpected DEB identity: %s", identity)
					}
				}
				if dependencies := run(t, "dpkg-deb", "-f", archive, "Depends"); strings.Contains(dependencies, "rclone") {
					t.Fatalf("DEB retains a system rclone dependency: %s", dependencies)
				}
				run(t, "dpkg-deb", "-x", archive, root)
				control := t.TempDir()
				run(t, "dpkg-deb", "-e", archive, control)
				if string(read(t, filepath.Join(control, "conffiles"))) != "/etc/backupchief/config.json\n" {
					t.Fatal("config is not a conffile")
				}
				for _, hook := range []string{"postinst", "prerm", "postrm"} {
					run(t, "sh", "-n", filepath.Join(control, hook))
				}
			} else {
				identity := run(t, "rpm", "-qp", "--qf", "%{NAME} %{ARCH} %{OS} %{LICENSE} %{PACKAGER}", archive)
				if identity != "backupchief "+target.arch+" linux Apache-2.0 Backup Chief Team <hello@chief.app>" {
					t.Fatalf("unexpected RPM identity: %s", identity)
				}
				if dependencies := run(t, "rpm", "-qp", "--requires", archive); strings.Contains(dependencies, "rclone") {
					t.Fatalf("RPM retains a system rclone dependency: %s", dependencies)
				}
				flags := run(t, "rpm", "-qp", "--qf", "[%{FILENAMES} %{FILEFLAGS:fflags}\n]", archive)
				if !strings.Contains(flags, "/etc/backupchief/config.json cn\n") {
					t.Fatalf("missing config/noreplace: %s", flags)
				}
				command := exec.Command("bash", "-o", "pipefail", "-c", `rpm2cpio "$1" | cpio -idm`, "extract", archive)
				command.Dir = root
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("extract: %v: %s", err, output)
				}
			}

			licensePath := "usr/share/doc/backupchief/restic-LICENSE"
			if target.format == "rpm" {
				licensePath = "usr/share/licenses/backupchief/restic-LICENSE"
			}
			if !bytes.Equal(read(t, filepath.Join(root, licensePath)), read(t, "../restic/LICENSE")) {
				t.Fatal("missing upstream restic license")
			}
			rcloneLicensePath := "usr/share/doc/backupchief/rclone-COPYING"
			if target.format == "rpm" {
				rcloneLicensePath = "usr/share/licenses/backupchief/rclone-COPYING"
			}
			if !bytes.Equal(read(t, filepath.Join(root, rcloneLicensePath)), read(t, "../rclone/COPYING")) {
				t.Fatal("missing upstream rclone license")
			}

			binary := filepath.Join(root, "usr/bin/backupchief")
			executable, err := elf.Open(binary)
			if err != nil {
				t.Fatal(err)
			}
			defer executable.Close()
			if executable.Machine != target.machine {
				t.Fatalf("ELF architecture: %s", executable.Machine)
			}
			for _, program := range executable.Progs {
				if program.Type == elf.PT_INTERP {
					t.Fatal("binary requires a dynamic interpreter")
				}
			}
			if !bytes.Equal(read(t, binary), read(t, filepath.Join(packageDirectory, "backupchief-linux-"+target.goarch))) {
				t.Fatal("packaged executable differs from the validated build")
			}
			for _, location := range []struct {
				path string
				mode os.FileMode
			}{
				{"usr/bin/backupchief", 0755},
				{"usr/lib/systemd/system/backupchief.service", 0644},
				{"etc/backupchief/config.json", 0640},
				{"etc/backupchief", 0750},
			} {
				info, err := os.Stat(filepath.Join(root, location.path))
				if err != nil {
					t.Fatal(err)
				}
				if info.Mode().Perm() != location.mode {
					t.Fatalf("%s mode %o, want %o", location.path, info.Mode().Perm(), location.mode)
				}
			}
			if !bytes.Equal(read(t, filepath.Join(root, "etc/backupchief/config.json")), read(t, "config.json")) {
				t.Fatal("unexpected bootstrap contents")
			}
			unit := string(read(t, filepath.Join(root, "usr/lib/systemd/system/backupchief.service")))
			for _, setting := range []string{
				"User=backupchief",
				"Group=backupchief",
				"ExecStart=/usr/bin/backupchief run",
				"RestartPreventExitStatus=2",
				"StateDirectory=backupchief",
				"LogsDirectory=backupchief",
			} {
				if !strings.Contains(unit, "\n"+setting+"\n") {
					t.Fatalf("packaged service is missing %q", setting)
				}
			}
		})
	}
}

func TestPostinstallRestartsOnlyAnActiveServiceDuringUpgrade(t *testing.T) {
	hook := string(read(t, "postinstall.sh"))
	for _, expected := range []string{
		`configure)`,
		`if [ -n "${2:-}" ]; then`,
		`2|3|4|5|6|7|8|9)`,
		`systemctl try-restart backupchief.service`,
	} {
		if !strings.Contains(hook, expected) {
			t.Fatalf("postinstall hook is missing %q", expected)
		}
	}
	if strings.Contains(hook, "systemctl restart backupchief.service") {
		t.Fatal("postinstall unconditionally restarts the service")
	}
}

func run(t *testing.T, name string, args ...string) string {
	t.Helper()
	command := exec.Command(name, args...)
	output, err := command.Output()
	if err != nil {
		t.Fatalf("%s %v: %v: %s", name, args, err, output)
	}
	return string(output)
}

func read(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
