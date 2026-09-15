package cmd

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/chieftools/backupchief-agent/agent"
	"github.com/chieftools/backupchief-agent/restic"
	"github.com/spf13/cobra"
)

func newExportCommand(configPath *string) *cobra.Command {
	var snapshot string
	var repositorySelector string
	var kind string
	var encodedPath string
	var output string

	command := &cobra.Command{
		Use:   "export <job>",
		Short: "Export one file, directory, or database snapshot as a ZIP",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			config, _, err := loadCommandConfiguration(*configPath)
			if err != nil {
				return err
			}
			job, err := agent.SelectJob(config, args[0])
			if err != nil {
				return err
			}
			if !job.Enabled {
				return errors.New("the selected backup job is not active")
			}
			repository, err := agent.SelectJobRepository(job, repositorySelector)
			if err != nil {
				return err
			}
			path, err := decodeSnapshotPath(encodedPath)
			if err != nil {
				return err
			}
			if err = validateJobSnapshotSelection(job, kind, path); err != nil {
				return err
			}

			request := exportRequest(job, repository, snapshot, kind, path)
			return writeSnapshotExport(command, stateForConfig(*configPath), request, output)
		},
	}
	command.Flags().StringVar(&snapshot, "snapshot", "", "Full snapshot ID")
	command.Flags().StringVar(&repositorySelector, "repository", "", "Repository key (defaults to the primary)")
	command.Flags().StringVar(&kind, "kind", "", "Selection kind: file, directory, or database")
	command.Flags().StringVar(&encodedPath, "path-base64", "", "Base64url-encoded absolute snapshot path")
	command.Flags().StringVarP(&output, "output", "o", "", "New .zip file to create")
	_ = command.MarkFlagRequired("snapshot")
	_ = command.MarkFlagRequired("kind")
	_ = command.MarkFlagRequired("path-base64")
	_ = command.MarkFlagRequired("output")

	return command
}

func exportRequest(job agent.Job, repository agent.JobRepository, snapshot, kind, path string) restic.ExportRequest {
	return restic.ExportRequest{
		Version: 1,
		Connection: restic.Connection{
			Driver: repository.Connection.Driver, Path: repository.Connection.Path,
			Endpoint: repository.Connection.Endpoint, Bucket: repository.Connection.Bucket,
			Prefix: repository.Connection.Prefix, Region: repository.Connection.Region,
			AccessKey: repository.Connection.AccessKey, SecretKey: repository.Connection.SecretKey,
		},
		Password: repository.ServicePassword, Snapshot: snapshot, Kind: kind, Path: path,
		ArchiveEntryName: exportEntryName(path), TimeoutSeconds: 6 * 60 * 60, LockWaitSeconds: 5 * 60,
	}
}

func writeSnapshotExport(command *cobra.Command, state string, request restic.ExportRequest, output string) error {
	if !strings.HasSuffix(strings.ToLower(output), ".zip") {
		return errors.New("export output must use the .zip extension")
	}
	output, err := filepath.Abs(output)
	if err != nil {
		return errors.New("invalid export output path")
	}
	if _, err = os.Lstat(output); err == nil || !os.IsNotExist(err) {
		return errors.New("export output already exists or cannot be inspected")
	}
	part := output + ".part"
	file, err := os.OpenFile(part, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return errors.New("cannot create private partial export")
	}
	complete := false
	defer func() {
		_ = file.Close()
		if !complete {
			_ = os.Remove(part)
		}
	}()

	runner := restic.Runner{State: state, AllowLocal: true}
	if err = runner.StreamExport(command.Context(), request, file); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return errors.New("cannot persist partial export")
	}
	if err = file.Close(); err != nil {
		return errors.New("cannot close partial export")
	}
	if err = chownSudoUser(part); err != nil {
		return err
	}
	if err = publishSnapshotExport(part, output); err != nil {
		return errors.New("cannot publish completed export")
	}
	complete = true

	_, err = fmt.Fprintf(command.OutOrStdout(), "Export written to %s\n", output)
	return err
}

func publishSnapshotExport(part, output string) error {
	if err := os.Link(part, output); err != nil {
		return err
	}
	if err := os.Remove(part); err != nil {
		_ = os.Remove(output)
		return err
	}

	return nil
}

func decodeSnapshotPath(encoded string) (string, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(decoded) == 0 || len(decoded) > 4096 || decoded[0] != '/' || strings.ContainsAny(string(decoded), "\\\x00") {
		return "", errors.New("invalid encoded snapshot path")
	}
	path := string(decoded)
	if path != filepath.Clean(path) {
		return "", errors.New("snapshot path must be normalized and absolute")
	}

	return path, nil
}

func validateJobSnapshotSelection(job agent.Job, kind, path string) error {
	if kind != "file" && kind != "directory" && kind != "database" {
		return errors.New("invalid snapshot selection kind")
	}
	if job.Type == agent.JobTypeFile {
		root := filepath.Clean(job.Source.Root)
		insideRoot := root == string(filepath.Separator) || path == root || strings.HasPrefix(path, root+string(filepath.Separator))
		if kind == "database" || !insideRoot {
			return errors.New("snapshot path is outside the configured backup root")
		}
		return nil
	}
	if kind != "database" || filepath.Dir(path) != "/" || !strings.HasSuffix(path, ".sql") {
		return errors.New("database exports require one SQL artifact")
	}

	return nil
}

func exportEntryName(path string) string {
	if path == "/" {
		return "backup"
	}
	return filepath.Base(path)
}

func chownSudoUser(path string) error {
	if os.Geteuid() != 0 || os.Getenv("SUDO_UID") == "" {
		return nil
	}
	uid, err := strconv.Atoi(os.Getenv("SUDO_UID"))
	if err != nil || uid < 0 {
		return errors.New("invalid SUDO_UID")
	}
	gid := -1
	if os.Getenv("SUDO_GID") != "" {
		gid, err = strconv.Atoi(os.Getenv("SUDO_GID"))
		if err != nil || gid < 0 {
			return errors.New("invalid SUDO_GID")
		}
	}
	if err = os.Chown(path, uid, gid); err != nil {
		return errors.New("cannot assign export to invoking user")
	}
	return nil
}
