package updater

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const packageName = "backupchief"

type CommandRunner interface {
	CombinedOutput(context.Context, string, ...string) ([]byte, error)
}

// aptEnvironment keeps apt and dpkg from prompting and needrestart from
// restarting services on its own. dnf and yum ignore it.
var aptEnvironment = []string{"DEBIAN_FRONTEND=noninteractive", "APT_LISTCHANGES_FRONTEND=none", "NEEDRESTART_SUSPEND=1"}

type execRunner struct{}

func (execRunner) CombinedOutput(ctx context.Context, name string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, args...)
	command.Env = append(os.Environ(), aptEnvironment...)
	return command.CombinedOutput()
}

type PackageManager interface {
	Name() string
	CurrentVersion(context.Context) (string, error)
	Refresh(context.Context) error
	CheckAvailable(context.Context, string) error
	Install(context.Context, string) error
	Downgrade(context.Context, string) error
}

func DetectPackageManager() (PackageManager, error) {
	return detectPackageManager(execRunner{})
}

func detectPackageManager(runner CommandRunner) (PackageManager, error) {
	if path, err := exec.LookPath("dnf"); err == nil {
		return &rpmManager{name: "dnf", binary: path, runner: runner}, nil
	}
	if path, err := exec.LookPath("yum"); err == nil {
		return &rpmManager{name: "yum", binary: path, runner: runner}, nil
	}
	if path, err := exec.LookPath("apt-get"); err == nil {
		return &aptManager{binary: path, runner: runner}, nil
	}
	return nil, fmt.Errorf("no supported package manager found")
}

type aptManager struct {
	binary string
	runner CommandRunner
}

func (manager *aptManager) Name() string { return "apt" }

func (manager *aptManager) CurrentVersion(ctx context.Context) (string, error) {
	output, err := manager.runner.CombinedOutput(ctx, "dpkg-query", "-W", "-f=${Version}", packageName)
	if err != nil {
		return "", commandError("query installed package", output, err)
	}
	return strings.TrimSpace(string(output)), nil
}

func (manager *aptManager) Refresh(ctx context.Context) error {
	refresh, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	output, err := manager.runner.CombinedOutput(refresh, manager.binary, "update")
	if err != nil {
		log.Printf("backupchief: apt metadata refresh reported an error; checking the requested version anyway: %v", commandError("refresh apt metadata", output, err))
	}
	return nil
}

func (manager *aptManager) CheckAvailable(ctx context.Context, version string) error {
	check, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	output, err := manager.runner.CombinedOutput(check, "apt-cache", "madison", packageName)
	if err != nil {
		return commandError("query apt repository", output, err)
	}
	for _, line := range strings.Split(string(output), "\n") {
		parts := strings.Split(line, "|")
		if len(parts) >= 2 && strings.TrimSpace(parts[1]) == version {
			return nil
		}
	}
	return fmt.Errorf("version %s is unavailable in the apt repository", version)
}

func (manager *aptManager) Install(ctx context.Context, version string) error {
	install, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	// Keep a locally changed config.json and take the package's version of
	// an unchanged one, since dpkg's conffile prompt cannot be answered here.
	// apt before 1.1 has no --allow-downgrades; its --force-yes allows
	// downgrades as well.
	downgrades := "--allow-downgrades"
	if versionOutput, err := manager.runner.CombinedOutput(install, manager.binary, "--version"); err == nil && aptVersionBefore11(string(versionOutput)) {
		downgrades = "--force-yes"
	}
	output, err := manager.runner.CombinedOutput(install, manager.binary, "install", "-y", downgrades,
		"-o", "Dpkg::Options::=--force-confdef", "-o", "Dpkg::Options::=--force-confold", packageName+"="+version)
	if err != nil {
		return commandError("install apt package", output, err)
	}
	return nil
}

func (manager *aptManager) Downgrade(ctx context.Context, version string) error {
	return manager.Install(ctx, version)
}

// aptVersionBefore11 parses the first line of apt-get --version, such as
// "apt 1.0.1ubuntu2.24 for amd64 compiled on ..." or "apt 2.8.3 (amd64)".
// Ubuntu 14.04 and Debian 8 ship apt 1.0.
func aptVersionBefore11(output string) bool {
	fields := strings.Fields(output)
	if len(fields) < 2 || fields[0] != "apt" {
		return false
	}
	parts := strings.SplitN(fields[1], ".", 3)
	if len(parts) < 2 {
		return false
	}
	major, majorErr := strconv.Atoi(parts[0])
	minor, minorErr := strconv.Atoi(strings.TrimRightFunc(parts[1], func(r rune) bool { return r < '0' || r > '9' }))
	if majorErr != nil || minorErr != nil {
		return false
	}
	return major < 1 || (major == 1 && minor < 1)
}

type rpmManager struct {
	name   string
	binary string
	runner CommandRunner
}

func (manager *rpmManager) Name() string { return manager.name }

func (manager *rpmManager) CurrentVersion(ctx context.Context) (string, error) {
	output, err := manager.runner.CombinedOutput(ctx, "rpm", "-q", "--qf", "%{VERSION}", packageName)
	if err != nil {
		return "", commandError("query installed package", output, err)
	}
	return strings.TrimSpace(string(output)), nil
}

func (manager *rpmManager) Refresh(ctx context.Context) error {
	refresh, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	output, err := manager.runner.CombinedOutput(refresh, manager.binary, "clean", "expire-cache", "--disablerepo=*", "--enablerepo=backupchief")
	if err != nil {
		return commandError("refresh rpm metadata", output, err)
	}
	return nil
}

func (manager *rpmManager) CheckAvailable(ctx context.Context, version string) error {
	check, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	output, err := manager.runner.CombinedOutput(check, manager.binary, "--disablerepo=*", "--enablerepo=backupchief", "--showduplicates", "list", packageName)
	if err != nil {
		return commandError("query rpm repository", output, err)
	}
	for _, field := range strings.Fields(string(output)) {
		if field == version || strings.HasPrefix(field, version+"-") {
			return nil
		}
	}
	return fmt.Errorf("version %s is unavailable in the %s repository", version, manager.name)
}

func (manager *rpmManager) Install(ctx context.Context, version string) error {
	return manager.change(ctx, "install", version)
}

func (manager *rpmManager) Downgrade(ctx context.Context, version string) error {
	return manager.change(ctx, "downgrade", version)
}

func (manager *rpmManager) change(ctx context.Context, action, version string) error {
	install, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	output, err := manager.runner.CombinedOutput(install, manager.binary, "--disablerepo=*", "--enablerepo=backupchief", action, "-y", packageName+"-"+version)
	if err != nil {
		return commandError(action+" rpm package", output, err)
	}
	return nil
}

func commandError(action string, output []byte, err error) error {
	diagnostic := strings.TrimSpace(string(output))
	if len(diagnostic) > 2048 {
		diagnostic = diagnostic[len(diagnostic)-2048:]
	}
	if diagnostic == "" {
		return fmt.Errorf("%s: %w", action, err)
	}
	return fmt.Errorf("%s: %w: %s", action, err, diagnostic)
}
