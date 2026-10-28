package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/chieftools/backupchief-agent/agent"
	"github.com/chieftools/backupchief-agent/initsys"
	"github.com/chieftools/backupchief-agent/logfile"
	"github.com/chieftools/backupchief-agent/updater"
	"github.com/spf13/cobra"
)

func NewRootCommand(version string) *cobra.Command {
	if version == "" {
		version = "dev"
	}

	root := &cobra.Command{
		Use:           "backupchief",
		Short:         "Backup Chief backup agent",
		Long:          "Run backups from a local configuration or connect this agent to Backup Chief.",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return command.Help()
		},
	}
	root.CompletionOptions.DisableDefaultCmd = true
	root.SetVersionTemplate("Backup Chief agent {{.Version}}\n")
	configPath := agent.DefaultPaths().StandaloneConfig
	root.PersistentFlags().StringVar(&configPath, "config", configPath, "Standalone configuration file")
	root.AddCommand(newResticCommand())
	root.AddCommand(newResticExportCommand())
	root.AddCommand(newStorageCommand())

	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Show the build version",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			_, err := fmt.Fprintf(command.OutOrStdout(), "Backup Chief agent %s\n", version)
			return err
		},
	})

	root.AddCommand(newSetupCommand(version, &configPath))
	root.AddCommand(newRunCommand(version, &configPath))
	root.AddCommand(newConfigCommand(version, &configPath))
	root.AddCommand(newBackupCommand(version, &configPath))
	root.AddCommand(newExportCommand(&configPath))
	root.AddCommand(newRestoreCommand(&configPath))
	root.AddCommand(newRepositoryCommand(&configPath))
	root.AddCommand(newInternalUpdateCommand())
	addDevelopmentCommands(root, version)

	return root
}

func newInternalUpdateCommand() *cobra.Command {
	command := &cobra.Command{
		Use:    "internal-update",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if os.Geteuid() != 0 {
				return fmt.Errorf("internal updater must run as root")
			}
			store, err := agent.NewSystemFileStore(agent.DefaultPaths())
			if err != nil {
				return err
			}
			return updater.Run(command.Context(), updater.Options{
				StateDirectory: filepath.Dir(agent.DefaultPaths().State),
				ServiceUID:     store.ServiceUID,
				ServiceGID:     store.ServiceGID,
			})
		},
	}
	return command
}

func newSetupCommand(version string, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "setup <token>",
		Short: "Set up this server with Backup Chief",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			store, err := agent.NewSystemFileStore(pathsForConfig(*configPath))
			if err != nil {
				return err
			}
			serviceManager, initSystem := agent.DetectServiceManager()
			bootstrap, err := agent.Enroll(command.Context(), args[0], agent.EnrollOptions{
				Store:          store,
				Version:        version,
				ServiceManager: serviceManager,
			})
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(command.OutOrStdout(), setupCompletion(bootstrap.ServerID, initSystem))
			return err
		},
	}
}

// setupCompletion tells how the agent runs after setup.
func setupCompletion(serverID string, initSystem initsys.Kind) string {
	switch initSystem {
	case initsys.Systemd:
		return fmt.Sprintf("Server %s is set up and backupchief.service started.", serverID)
	case initsys.SysV, initsys.Upstart:
		return fmt.Sprintf("Server %s is set up and the backupchief service started, also at boot.", serverID)
	default:
		return fmt.Sprintf("Server %s is set up. No init system manages Backup Chief here, start it with:\n  backupchief run --log-file /var/log/backupchief/agent.log", serverID)
	}
}

func newRunCommand(version string, configPath *string) *cobra.Command {
	var logFile string
	command := &cobra.Command{
		Use:   "run",
		Short: "Run the backup daemon",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if logFile != "" {
				file, err := logfile.Setup(logFile)
				if err != nil {
					return fmt.Errorf("open log file %s: %w", logFile, err)
				}
				defer file.Close()
				reopenLogOnHangup(command.Context(), file)
			}
			store, err := agent.NewSystemFileStore(pathsForConfig(*configPath))
			if err != nil {
				return err
			}
			return agent.Run(command.Context(), agent.RunOptions{
				Store:   store,
				Version: version,
			})
		},
	}
	command.Flags().StringVar(&logFile, "log-file", "", "Log to this file with size-capped rotation instead of stderr (for hosts without systemd)")
	return command
}

// reopenLogOnHangup reopens the log file on SIGHUP, which the init script's
// reload sends, so a log rotated away by hand is followed. Without a handler
// SIGHUP would stop the agent.
func reopenLogOnHangup(ctx context.Context, file *logfile.RotatingFile) {
	hangups := make(chan os.Signal, 1)
	signal.Notify(hangups, syscall.SIGHUP)
	go func() {
		defer signal.Stop(hangups)
		for {
			select {
			case <-ctx.Done():
				return
			case <-hangups:
				if err := file.Reopen(); err != nil {
					log.Printf("backupchief: reopen log file: %v", err)
				}
			}
		}
	}()
}

func pathsForConfig(configPath string) agent.Paths {
	paths := agent.DefaultPaths()
	if configPath == paths.StandaloneConfig {
		return paths
	}
	directory := filepath.Dir(configPath)
	paths.Identity = filepath.Join(directory, "identity.json")
	paths.Pending = filepath.Join(directory, "setup.json")
	paths.StandaloneConfig = configPath
	paths.ManagedConfig = filepath.Join(directory, "managed-config.json")
	return paths
}
