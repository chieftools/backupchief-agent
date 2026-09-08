package cmd

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/chieftools/backupchief-agent/agent"
	"github.com/chieftools/backupchief-agent/restic"
	"github.com/spf13/cobra"
)

func newBackupCommand(configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "backup <job>",
		Short: "Run a configured backup and wait for it to finish",
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
			if config.Host.Key != "" {
				return runManagedBackup(command, config, job)
			}
			request := agent.ResticRequest(job, "backup", config.Host.Name)
			request.Tags = append(request.Tags, "backupchief-run:manual")
			return runDirectRestic(command, *configPath, request)
		},
	}
}

func newRepositoryCommand(configPath *string) *cobra.Command {
	command := &cobra.Command{
		Use:   "repository",
		Short: "Manage configured repositories",
		Args:  cobra.NoArgs,
	}
	command.AddCommand(&cobra.Command{
		Use:   "init <job>",
		Short: "Initialize a standalone job repository",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			config, _, err := loadCommandConfiguration(*configPath)
			if err != nil {
				return err
			}
			if config.Host.Key != "" {
				return errors.New("managed repositories are initialized by Backup Chief")
			}
			job, err := agent.SelectJob(config, args[0])
			if err != nil {
				return err
			}
			return runDirectRestic(command, *configPath, agent.ResticRequest(job, "init", config.Host.Name))
		},
	})
	return command
}

func runDirectRestic(command *cobra.Command, configPath string, request restic.Request) error {
	state := "/var/lib/backupchief"
	if configPath != agent.DefaultPaths().StandaloneConfig && configPath != agent.DefaultPaths().ManagedConfig {
		state = filepath.Join(filepath.Dir(configPath), ".backupchief-state")
	}
	runner := restic.Runner{
		State:      state,
		AllowLocal: true,
	}
	result := runner.Run(command.Context(), request)
	if result.Output != "" {
		_, _ = fmt.Fprint(command.OutOrStdout(), result.Output)
	}
	if result.Diagnostic != "" {
		_, _ = fmt.Fprintln(command.ErrOrStderr(), result.Diagnostic)
	}
	if result.Outcome != "complete" {
		if errors.Is(command.Context().Err(), context.Canceled) {
			return operationError(130, "restic %s was cancelled", request.Operation)
		}
		if result.Outcome == "partial" || result.ExitCode == 3 {
			return operationError(3, "restic %s completed with unreadable files", request.Operation)
		}
		return operationError(1, "restic %s failed with exit code %d", request.Operation, result.ExitCode)
	}
	return nil
}

func loadCommandConfiguration(configPath string) (agent.Config, agent.ConfigMetadata, error) {
	paths := pathsForConfig(configPath)
	store := agent.NewUserFileStore(paths)
	bootstrap, err := store.LoadBootstrap()
	if errors.Is(err, agent.ErrManagedIdentityMissing) {
		return agent.LoadConfiguration(paths.StandaloneConfig)
	}
	if err != nil {
		return agent.Config{}, agent.ConfigMetadata{}, fmt.Errorf("load managed identity: %w", err)
	}
	body, metadata, err := store.LoadConfig(bootstrap)
	if err != nil {
		return agent.Config{}, metadata, err
	}
	config, _, err := agent.DecodeConfig(body, bootstrap.Generation)
	if err != nil {
		return agent.Config{}, metadata, err
	}
	config.Host.Key = bootstrap.Credential
	config.Host.Endpoint = bootstrap.Endpoint
	config.Host.SetupTokenHash = bootstrap.SetupTokenHash
	return config, metadata, nil
}

func runManagedBackup(command *cobra.Command, config agent.Config, job agent.Job) error {
	client := agent.NewClient(config.Host.Endpoint, config.Host.Key, "dev", nil)
	status, err := client.StartManagedBackup(command.Context(), job.ID)
	if err != nil {
		return operationError(1, "queue managed backup: %w", err)
	}
	_, _ = fmt.Fprintf(command.OutOrStdout(), "Backup queued as %s. Waiting for completion…\n", status.ID)
	status, err = client.WaitForManagedBackup(command.Context(), status.ID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return operationError(130, "backup was cancelled: %w", err)
		}
		return operationError(1, "wait for managed backup: %w", err)
	}
	_, _ = fmt.Fprintf(command.OutOrStdout(), "Backup %s: %s", status.ID, status.Status)
	if status.Summary != "" {
		_, _ = fmt.Fprintf(command.OutOrStdout(), " — %s", status.Summary)
	}
	_, _ = fmt.Fprintln(command.OutOrStdout())
	if status.Status != "complete" {
		if status.Status == "partial" {
			return operationError(3, "backup finished with status %s", status.Status)
		}
		return operationError(1, "backup finished with status %s", status.Status)
	}
	return nil
}
