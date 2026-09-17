package updater

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"
)

const packageName = "backupchief"

type CommandRunner interface {
	CombinedOutput(context.Context, string, ...string) ([]byte, error)
}

type execRunner struct{}

func (execRunner) CombinedOutput(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
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
	output, err := manager.runner.CombinedOutput(install, manager.binary, "install", "-y", "--allow-downgrades", packageName+"="+version)
	if err != nil {
		return commandError("install apt package", output, err)
	}
	return nil
}

func (manager *aptManager) Downgrade(ctx context.Context, version string) error {
	return manager.Install(ctx, version)
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
