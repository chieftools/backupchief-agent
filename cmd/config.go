package cmd

import (
	"fmt"
	"net/http"

	"github.com/chieftools/backupchief-agent/agent"
	"github.com/spf13/cobra"
)

func newConfigCommand(version string, configPath *string) *cobra.Command {
	command := &cobra.Command{
		Use:   "config",
		Short: "Inspect or update the agent configuration",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return command.Help()
		},
	}

	command.AddCommand(&cobra.Command{
		Use:   "validate",
		Short: "Validate the configuration without running a backup",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			config, _, err := loadCommandConfiguration(*configPath)
			if err != nil {
				return err
			}

			mode := "standalone"
			if config.Host.Key != "" {
				mode = "managed"
			}

			for _, warning := range config.Warnings {
				if _, err := fmt.Fprintf(command.OutOrStdout(), "Configuration warning: %s\n", warning); err != nil {
					return err
				}
			}

			_, err = fmt.Fprintf(command.OutOrStdout(), "Configuration is valid (%s, %d jobs).\n", mode, len(config.Jobs))
			return err
		},
	})

	command.AddCommand(newConfigUpdateCommand(version, func() (*agent.FileStore, error) {
		return agent.NewSystemFileStore(pathsForConfig(*configPath))
	}, nil, nil))
	return command
}

func newConfigUpdateCommand(
	version string,
	storeFactory func() (*agent.FileStore, error),
	httpClientFactory func() *http.Client,
	validateStore func(*agent.FileStore) error,
) *cobra.Command {
	force := false
	command := &cobra.Command{
		Use:   "update",
		Short: "Fetch and validate the latest configuration",
		Long:  "Fetch and validate the latest managed configuration, then atomically replace the local file. Use --force to bypass the current ETag.",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			store, err := storeFactory()
			if err != nil {
				return err
			}

			if validateStore != nil {
				if err := validateStore(store); err != nil {
					return err
				}
			}

			var httpClient *http.Client
			if httpClientFactory != nil {
				httpClient = httpClientFactory()
				defer httpClient.CloseIdleConnections()
			}

			result, err := agent.UpdateConfig(command.Context(), agent.ConfigUpdateOptions{
				Store: store, Version: version, HTTPClient: httpClient, Force: force,
			})
			if err != nil {
				return err
			}

			status := "already up to date"
			if result.Updated {
				status = "updated"
			}

			_, err = fmt.Fprintf(
				command.OutOrStdout(),
				"Configuration %s: generation %d, revision %d, digest %s.\n",
				status,
				result.Metadata.Generation,
				result.Metadata.Revision,
				result.Metadata.Digest,
			)
			return err
		},
	}

	command.Flags().BoolVarP(&force, "force", "f", false, "Bypass the current configuration ETag")
	return command
}
