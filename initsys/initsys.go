// Package initsys detects the host's init system and manages the agent
// service through it. Managers only shell out to the init system's own
// tools; they never touch unit files or runlevel links directly.
package initsys

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"strings"
)

// Kind identifies an init system.
type Kind string

const (
	Systemd Kind = "systemd"
	Upstart Kind = "upstart"
	SysV    Kind = "sysv"
	Unknown Kind = "unknown"
)

// DefaultService is the agent's service name.
const DefaultService = "backupchief"

// Detect inspects a file system rooted at "/" and reports the running init
// system. It follows sd_booted(3): systemd is running when
// /run/systemd/system exists. Upstart is recognised by its control tool
// and job directory, and anything else with /etc/init.d is SysV.
func Detect(fsys fs.FS) Kind {
	if isDir(fsys, "run/systemd/system") {
		return Systemd
	}
	if comm := readTrimmed(fsys, "proc/1/comm"); comm == "systemd" {
		return Systemd
	}
	if exists(fsys, "sbin/initctl") && isDir(fsys, "etc/init") && upstartInit(fsys) {
		return Upstart
	}
	if isDir(fsys, "etc/init.d") || isDir(fsys, "etc/rc.d/init.d") {
		return SysV
	}
	return Unknown
}

// DetectSystem detects the init system of the running machine.
func DetectSystem() Kind {
	return Detect(os.DirFS("/"))
}

// upstartInit separates Upstart from SysV hosts that merely ship a
// compatibility initctl (Debian's sysvinit does not, but some images do).
func upstartInit(fsys fs.FS) bool {
	if target, err := fs.ReadLink(fsys, "proc/1/exe"); err == nil && strings.Contains(target, "upstart") {
		return true
	}
	if target, err := fs.ReadLink(fsys, "sbin/init"); err == nil && strings.Contains(target, "upstart") {
		return true
	}
	if exists(fsys, "usr/share/upstart") || exists(fsys, "etc/init/rc-sysinit.conf") {
		return true
	}
	return false
}

// Runner executes a command. Tests substitute a recorder.
type Runner interface {
	Run(ctx context.Context, name string, args ...string) error
}

// ExecRunner runs commands with os/exec.
type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, name string, args ...string) error {
	command := exec.CommandContext(ctx, name, args...)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Run(); err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(output.String()))
	}
	return nil
}

// Manager controls the agent service.
type Manager interface {
	Kind() Kind
	Enable(ctx context.Context) error
	Disable(ctx context.Context) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Restart(ctx context.Context) error
}

// New returns the manager for kind. Upstart hosts (Ubuntu 14.04) run SysV
// scripts through their compatibility layer, so they get the SysV manager.
func New(kind Kind, fsys fs.FS, service string, runner Runner) (Manager, error) {
	if service == "" {
		service = DefaultService
	}
	if runner == nil {
		runner = ExecRunner{}
	}
	switch kind {
	case Systemd:
		return &SystemdManager{Unit: service + ".service", Runner: runner}, nil
	case SysV, Upstart:
		return NewSysVManager(fsys, service, runner), nil
	default:
		return nil, fmt.Errorf("unsupported init system %q", kind)
	}
}

// SystemdManager drives a unit with systemctl.
type SystemdManager struct {
	Unit   string
	Runner Runner
}

func (manager *SystemdManager) Kind() Kind { return Systemd }

func (manager *SystemdManager) Enable(ctx context.Context) error {
	if err := manager.Runner.Run(ctx, "systemctl", "daemon-reload"); err != nil {
		return err
	}
	return manager.Runner.Run(ctx, "systemctl", "enable", manager.Unit)
}

func (manager *SystemdManager) Disable(ctx context.Context) error {
	return manager.Runner.Run(ctx, "systemctl", "disable", manager.Unit)
}

func (manager *SystemdManager) Start(ctx context.Context) error {
	return manager.Runner.Run(ctx, "systemctl", "start", manager.Unit)
}

func (manager *SystemdManager) Stop(ctx context.Context) error {
	return manager.Runner.Run(ctx, "systemctl", "stop", manager.Unit)
}

func (manager *SystemdManager) Restart(ctx context.Context) error {
	return manager.Runner.Run(ctx, "systemctl", "restart", manager.Unit)
}

// LinkTool is the runlevel-link tool a SysV host provides.
type LinkTool string

const (
	UpdateRCD LinkTool = "update-rc.d"
	Chkconfig LinkTool = "chkconfig"
)

// SysVManager drives /etc/init.d/<service> and maintains runlevel links
// with update-rc.d (Debian family) or chkconfig (RHEL family).
type SysVManager struct {
	Service string
	Script  string
	Tool    LinkTool
	Runner  Runner
}

// NewSysVManager picks update-rc.d when the host has it, otherwise chkconfig.
func NewSysVManager(fsys fs.FS, service string, runner Runner) *SysVManager {
	tool := Chkconfig
	if fsys != nil && (exists(fsys, "usr/sbin/update-rc.d") || exists(fsys, "sbin/update-rc.d")) {
		tool = UpdateRCD
	}
	return &SysVManager{
		Service: service,
		Script:  "/etc/init.d/" + service,
		Tool:    tool,
		Runner:  runner,
	}
}

func (manager *SysVManager) Kind() Kind { return SysV }

func (manager *SysVManager) Enable(ctx context.Context) error {
	if manager.Tool == UpdateRCD {
		return manager.Runner.Run(ctx, "update-rc.d", manager.Service, "defaults")
	}
	if err := manager.Runner.Run(ctx, "chkconfig", "--add", manager.Service); err != nil {
		return err
	}
	return manager.Runner.Run(ctx, "chkconfig", manager.Service, "on")
}

func (manager *SysVManager) Disable(ctx context.Context) error {
	if manager.Tool == UpdateRCD {
		return manager.Runner.Run(ctx, "update-rc.d", "-f", manager.Service, "remove")
	}
	return manager.Runner.Run(ctx, "chkconfig", "--del", manager.Service)
}

func (manager *SysVManager) Start(ctx context.Context) error {
	return manager.Runner.Run(ctx, manager.Script, "start")
}

func (manager *SysVManager) Stop(ctx context.Context) error {
	return manager.Runner.Run(ctx, manager.Script, "stop")
}

func (manager *SysVManager) Restart(ctx context.Context) error {
	return manager.Runner.Run(ctx, manager.Script, "restart")
}

func exists(fsys fs.FS, name string) bool {
	_, err := fs.Stat(fsys, name)
	return err == nil
}

func isDir(fsys fs.FS, name string) bool {
	info, err := fs.Stat(fsys, name)
	return err == nil && info.IsDir()
}

func readTrimmed(fsys fs.FS, name string) string {
	data, err := fs.ReadFile(fsys, name)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}
