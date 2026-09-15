package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/chieftools/backupchief-agent/agent"
	"github.com/chieftools/backupchief-agent/restic"
	"github.com/spf13/cobra"
)

func newRestoreCommand(configPath *string) *cobra.Command {
	var snapshot string
	var encodedPath string
	var target string

	command := &cobra.Command{
		Use:   "restore <job>",
		Short: "Restore a snapshot into an empty directory",
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
			target, err = validateRestoreTarget(target)
			if err != nil {
				return err
			}

			request, err := restoreRequest(job, snapshot, target, encodedPath)
			if err != nil {
				return err
			}
			if err = runDirectRestic(command, *configPath, request); err != nil {
				return err
			}

			_, err = fmt.Fprintf(command.OutOrStdout(), "Snapshot restored to %s\n", target)
			return err
		},
	}
	command.Flags().StringVar(&snapshot, "snapshot", "", "Full snapshot ID")
	command.Flags().StringVar(&encodedPath, "path-base64", "", "Optional base64url-encoded snapshot directory")
	command.Flags().StringVar(&target, "target", "", "Empty or non-existing restore directory")
	_ = command.MarkFlagRequired("snapshot")
	_ = command.MarkFlagRequired("target")

	return command
}

func restoreRequest(job agent.Job, snapshot, target, encodedPath string) (restic.Request, error) {
	request := agent.ResticRequest(job, "restore", "")
	request.Snapshot = snapshot
	request.Target = target
	request.Path = "/"
	if job.Type == agent.JobTypeFile {
		request.Path = filepath.Clean(job.Source.Root)
	}
	if encodedPath == "" {
		return request, nil
	}
	if job.Type != agent.JobTypeFile {
		return restic.Request{}, errors.New("a restore path is only supported for file backup jobs")
	}

	path, err := decodeSnapshotPath(encodedPath)
	if err != nil {
		return restic.Request{}, err
	}
	if err = validateJobSnapshotSelection(job, "directory", path); err != nil {
		return restic.Request{}, err
	}
	request.Path = path

	return request, nil
}

func validateRestoreTarget(target string) (string, error) {
	if target == "" {
		return "", errors.New("restore target is required")
	}

	target, err := filepath.Abs(target)
	if err != nil || target == string(filepath.Separator) {
		return "", errors.New("restore target must be a safe directory below the filesystem root")
	}

	info, err := os.Lstat(target)
	if os.IsNotExist(err) {
		parent, parentErr := os.Stat(filepath.Dir(target))
		if parentErr != nil || !parent.IsDir() {
			return "", errors.New("restore target parent must be an existing directory")
		}

		return target, nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("restore target must be an empty directory or must not exist")
	}

	entries, err := os.ReadDir(target)
	if err != nil || len(entries) != 0 {
		return "", errors.New("restore target must be an empty directory or must not exist")
	}

	return target, nil
}
