package agent

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/chieftools/backupchief-agent/restic"
)

var (
	capabilitiesOnce sync.Once
	capabilities     map[string]any
)

func probeCapabilities() map[string]any {
	capabilitiesOnce.Do(func() {
		mysql := probeExternalTool("mysql")
		mysqldump := probeExternalTool("mysqldump")
		mysqlAvailable := mysql["available"] == true && mysqldump["available"] == true
		psql := probeExternalTool("psql")
		pgDump := probeExternalTool("pg_dump")
		postgresqlAvailable := psql["available"] == true && pgDump["available"] == true
		capabilities = map[string]any{
			"backup_types": map[string]any{
				"file": map[string]any{"available": true},
				"mysql": map[string]any{
					"available": mysqlAvailable,
					"reason":    capabilityReason(mysqlAvailable, "mysql and mysqldump"),
					"tools":     map[string]any{"mysql": mysql, "mysqldump": mysqldump},
				},
				"postgresql": map[string]any{
					"available": postgresqlAvailable,
					"reason":    capabilityReason(postgresqlAvailable, "psql and pg_dump"),
					"tools":     map[string]any{"psql": psql, "pg_dump": pgDump},
				},
			},
			"tools": map[string]any{
				"restic":          map[string]any{"available": true, "bundled": true, "version": restic.Version},
				"snapshot_export": map[string]any{"available": true, "archive": "zip"},
				"mysql":           mysql, "mysqldump": mysqldump, "psql": psql, "pg_dump": pgDump,
			},
		}
	})

	return capabilities
}

func probeExternalTool(name string) map[string]any {
	resolved, err := resolveExternalTool(name)
	if err != nil {
		return map[string]any{"available": false}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, resolved, "--version").CombinedOutput()
	if err != nil {
		return map[string]any{"available": false, "path": resolved}
	}
	version := strings.TrimSpace(string(output))
	if len(version) > 255 {
		version = version[:255]
	}
	return map[string]any{"available": true, "path": resolved, "version": version}
}

func resolveExternalTool(name string) (string, error) {
	path, err := exec.LookPath(name)
	if err != nil {
		return "", err
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return "", fmt.Errorf("tool is not an executable regular file")
	}
	return path, nil
}

func capabilityReason(available bool, tools string) string {
	if available {
		return ""
	}
	return "Install executable " + tools + " client tools on the server."
}
