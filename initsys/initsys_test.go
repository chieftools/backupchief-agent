package initsys

import (
	"context"
	"errors"
	"io/fs"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
)

type recordingRunner struct {
	commands []string
	failOn   string
}

func (runner *recordingRunner) Run(_ context.Context, name string, args ...string) error {
	command := strings.Join(append([]string{name}, args...), " ")
	runner.commands = append(runner.commands, command)
	if runner.failOn != "" && command == runner.failOn {
		return errors.New("synthetic failure")
	}
	return nil
}

func dir() *fstest.MapFile { return &fstest.MapFile{Mode: fs.ModeDir | 0o755} }

func TestDetect(t *testing.T) {
	tests := []struct {
		name string
		fsys fstest.MapFS
		want Kind
	}{
		{
			name: "systemd booted",
			fsys: fstest.MapFS{"run/systemd/system": dir(), "etc/init.d": dir()},
			want: Systemd,
		},
		{
			name: "upstart on ubuntu 14.04",
			fsys: fstest.MapFS{
				"sbin/initctl":             &fstest.MapFile{Data: []byte{}},
				"etc/init/rc-sysinit.conf": &fstest.MapFile{Data: []byte("start on filesystem\n")},
				"etc/init.d":               dir(),
			},
			want: Upstart,
		},
		{
			name: "initctl without upstart jobs is sysv",
			fsys: fstest.MapFS{
				"sbin/initctl": &fstest.MapFile{Data: []byte{}},
				"etc/init":     dir(),
				"etc/init.d":   dir(),
			},
			want: SysV,
		},
		{
			name: "debian sysvinit",
			fsys: fstest.MapFS{"etc/init.d": dir(), "proc/1/comm": &fstest.MapFile{Data: []byte("init\n")}},
			want: SysV,
		},
		{
			name: "rhel rc.d layout",
			fsys: fstest.MapFS{"etc/rc.d/init.d": dir()},
			want: SysV,
		},
		{
			name: "nothing",
			fsys: fstest.MapFS{},
			want: Unknown,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := Detect(test.fsys); got != test.want {
				t.Fatalf("Detect() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSystemdCommands(t *testing.T) {
	runner := &recordingRunner{}
	manager, err := New(Systemd, fstest.MapFS{}, "", runner)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, step := range []func(context.Context) error{manager.Enable, manager.Start, manager.Restart, manager.Stop, manager.Disable} {
		if err := step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{
		"systemctl daemon-reload",
		"systemctl enable backupchief.service",
		"systemctl start backupchief.service",
		"systemctl restart backupchief.service",
		"systemctl stop backupchief.service",
		"systemctl disable backupchief.service",
	}
	if !reflect.DeepEqual(runner.commands, want) {
		t.Fatalf("commands = %#v, want %#v", runner.commands, want)
	}
	if manager.Kind() != Systemd {
		t.Fatalf("Kind() = %q", manager.Kind())
	}
}

func TestSysVUpdateRCDCommands(t *testing.T) {
	runner := &recordingRunner{}
	fsys := fstest.MapFS{"usr/sbin/update-rc.d": &fstest.MapFile{Data: []byte{}}}
	manager, err := New(SysV, fsys, "backupchief", runner)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, step := range []func(context.Context) error{manager.Enable, manager.Start, manager.Restart, manager.Stop, manager.Disable} {
		if err := step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{
		"update-rc.d backupchief defaults",
		"/etc/init.d/backupchief start",
		"/etc/init.d/backupchief restart",
		"/etc/init.d/backupchief stop",
		"update-rc.d -f backupchief remove",
	}
	if !reflect.DeepEqual(runner.commands, want) {
		t.Fatalf("commands = %#v, want %#v", runner.commands, want)
	}
}

func TestSysVChkconfigCommands(t *testing.T) {
	runner := &recordingRunner{}
	manager, err := New(SysV, fstest.MapFS{"sbin/chkconfig": &fstest.MapFile{Data: []byte{}}}, "backupchief", runner)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := manager.Enable(ctx); err != nil {
		t.Fatal(err)
	}
	if err := manager.Disable(ctx); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"chkconfig --add backupchief",
		"chkconfig backupchief on",
		"chkconfig --del backupchief",
	}
	if !reflect.DeepEqual(runner.commands, want) {
		t.Fatalf("commands = %#v, want %#v", runner.commands, want)
	}
}

func TestChkconfigEnableStopsAfterFailure(t *testing.T) {
	runner := &recordingRunner{failOn: "chkconfig --add backupchief"}
	manager := NewSysVManager(fstest.MapFS{}, "backupchief", runner)
	if err := manager.Enable(context.Background()); err == nil {
		t.Fatal("Enable() succeeded, want error")
	}
	if len(runner.commands) != 1 {
		t.Fatalf("commands = %#v, want only the failed --add", runner.commands)
	}
}

func TestUpstartUsesSysVManager(t *testing.T) {
	manager, err := New(Upstart, fstest.MapFS{"usr/sbin/update-rc.d": &fstest.MapFile{}}, "", &recordingRunner{})
	if err != nil {
		t.Fatal(err)
	}
	sysv, ok := manager.(*SysVManager)
	if !ok || sysv.Tool != UpdateRCD || sysv.Script != "/etc/init.d/backupchief" {
		t.Fatalf("manager = %#v", manager)
	}
}

func TestUnknownInitSystem(t *testing.T) {
	if _, err := New(Unknown, fstest.MapFS{}, "", nil); err == nil {
		t.Fatal("New(Unknown) succeeded, want error")
	}
}
