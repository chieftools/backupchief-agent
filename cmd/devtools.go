//go:build devtools

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/chieftools/backupchief-agent/agent"
	"github.com/spf13/cobra"
)

const defaultDevelopmentEndpoint = "https://backup.chief.test/agent/v1"

type developmentServiceManager struct{}

func (developmentServiceManager) Stop(context.Context) error {
	return nil
}

func (developmentServiceManager) EnableAndStart(context.Context) error {
	return nil
}

type developmentTransport struct {
	base   http.RoundTripper
	output io.Writer
	mu     sync.Mutex
}

func (transport *developmentTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	started := time.Now()
	response, err := transport.base.RoundTrip(request)
	duration := time.Since(started).Round(time.Millisecond)

	transport.mu.Lock()
	defer transport.mu.Unlock()
	if err != nil {
		_, _ = fmt.Fprintf(transport.output, "%s %s failed after %s: %v\n", request.Method, request.URL.EscapedPath(), duration, err)
		return nil, err
	}
	_, _ = fmt.Fprintf(transport.output, "%s %s -> %d in %s\n", request.Method, request.URL.EscapedPath(), response.StatusCode, duration)
	return response, nil
}

func addDevelopmentCommands(root *cobra.Command, version string) {
	root.AddCommand(newDevelopmentCommand(version))
}

func newDevelopmentCommand(version string) *cobra.Command {
	stateDirectory := ".backupchief-dev/default"
	command := &cobra.Command{
		Use:   "dev",
		Short: "Run an unprivileged local development agent",
		Long: `Set up and run a simulated Linux agent against a local control plane.

Development state stays in a user-owned directory. This command does not create
a service account, write system paths, install a package, or invoke systemd.`,
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return command.Help()
		},
	}
	command.PersistentFlags().StringVar(&stateDirectory, "state-dir", stateDirectory, "Directory for this development agent's identity and state")
	command.AddCommand(
		newDevelopmentSetupCommand(version, &stateDirectory),
		newDevelopmentRunCommand(version, &stateDirectory),
		newDevelopmentConfigCommand(version, &stateDirectory),
		newDevelopmentStatusCommand(&stateDirectory),
	)
	return command
}

func newDevelopmentConfigCommand(version string, stateDirectory *string) *cobra.Command {
	command := &cobra.Command{
		Use:   "config",
		Short: "Manage the cached development configuration",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return command.Help()
		},
	}
	command.AddCommand(newConfigUpdateCommand(
		version,
		func() (*agent.FileStore, error) {
			store, _, err := developmentStore(*stateDirectory)
			return store, err
		},
		func() *http.Client {
			return newDevelopmentHTTPClient(command.ErrOrStderr())
		},
		func(store *agent.FileStore) error {
			bootstrap, err := store.LoadBootstrap()
			if err != nil {
				return fmt.Errorf("load development identity: %w", err)
			}
			if err := validateDevelopmentEndpoint(bootstrap.Endpoint); err != nil {
				return fmt.Errorf("stored development endpoint is invalid: %w", err)
			}
			return nil
		},
	))
	return command
}

func newDevelopmentSetupCommand(version string, stateDirectory *string) *cobra.Command {
	endpoint := defaultDevelopmentEndpoint
	command := &cobra.Command{
		Use:   "setup <token>",
		Short: "Set up a local development agent",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			if err := validateDevelopmentEndpoint(endpoint); err != nil {
				return err
			}
			if runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64" {
				return fmt.Errorf("development setup requires an amd64 or arm64 host")
			}

			store, directory, err := developmentStore(*stateDirectory)
			if err != nil {
				return err
			}
			client := newDevelopmentHTTPClient(command.ErrOrStderr())
			defer client.CloseIdleConnections()

			bootstrap, err := agent.Enroll(command.Context(), args[0], agent.EnrollOptions{
				Store:          store,
				Endpoint:       endpoint,
				Version:        version,
				Platform:       "linux",
				Architecture:   runtime.GOARCH,
				HTTPClient:     client,
				ServiceManager: developmentServiceManager{},
				RootCheck:      func() bool { return true },
			})
			if err != nil {
				if _, pendingErr := os.Stat(store.Paths.Pending); pendingErr == nil {
					return fmt.Errorf("%w; setup state was preserved in %s, rerun with the same state directory and token to resume", err, directory)
				}
				return err
			}

			_, metadata, err := store.LoadConfig(bootstrap)
			if err != nil {
				return fmt.Errorf("load accepted development configuration: %w", err)
			}
			_, err = fmt.Fprintf(
				command.OutOrStdout(),
				"Development agent %s set up as linux/%s.\nState directory: %s\nAccepted config: generation %d, revision %d, digest %s\nNo package or service was installed.\n",
				bootstrap.ServerID,
				runtime.GOARCH,
				directory,
				metadata.Generation,
				metadata.Revision,
				metadata.Digest,
			)
			return err
		},
	}
	command.Flags().StringVar(&endpoint, "endpoint", endpoint, "Local control-plane agent endpoint")
	return command
}

func newDevelopmentRunCommand(version string, stateDirectory *string) *cobra.Command {
	heartbeatEvery := 5 * time.Second
	configEvery := 5 * time.Second
	commandEvery := 2 * time.Second
	command := &cobra.Command{
		Use:   "run",
		Short: "Run the local development agent in the foreground",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if heartbeatEvery <= 0 || configEvery <= 0 || commandEvery <= 0 {
				return fmt.Errorf("development polling intervals must be greater than zero")
			}

			store, directory, err := developmentStore(*stateDirectory)
			if err != nil {
				return err
			}
			bootstrap, err := store.LoadBootstrap()
			if err != nil {
				return fmt.Errorf("load development identity: %w", err)
			}
			_, metadata, err := store.LoadConfig(bootstrap)
			if err != nil {
				return fmt.Errorf("load accepted development configuration: %w", err)
			}
			if err := validateDevelopmentEndpoint(bootstrap.Endpoint); err != nil {
				return fmt.Errorf("stored development endpoint is invalid: %w", err)
			}

			_, err = fmt.Fprintf(
				command.OutOrStdout(),
				"Running development agent %s against %s.\nState directory: %s\nAccepted config: generation %d, revision %d, digest %s\nPolling: commands %s, heartbeat %s, config %s. Press Ctrl+C to stop.\n",
				bootstrap.ServerID,
				bootstrap.Endpoint,
				directory,
				metadata.Generation,
				metadata.Revision,
				metadata.Digest,
				commandEvery,
				heartbeatEvery,
				configEvery,
			)
			if err != nil {
				return err
			}

			client := newDevelopmentHTTPClient(command.ErrOrStderr())
			defer client.CloseIdleConnections()
			return agent.Run(command.Context(), agent.RunOptions{
				Store:          store,
				Version:        version,
				HTTPClient:     client,
				HeartbeatEvery: heartbeatEvery,
				ConfigEvery:    configEvery,
				CommandEvery:   commandEvery,
				AllowLocal:     true,
			})
		},
	}
	command.Flags().DurationVar(&heartbeatEvery, "heartbeat-every", heartbeatEvery, "Development heartbeat interval")
	command.Flags().DurationVar(&configEvery, "config-every", configEvery, "Development config-refresh interval")
	command.Flags().DurationVar(&commandEvery, "commands-every", commandEvery, "Development command-poll interval")
	return command
}

func newDevelopmentStatusCommand(stateDirectory *string) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show local development agent state without secrets",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			store, directory, err := developmentStore(*stateDirectory)
			if err != nil {
				return err
			}

			bootstrap, err := store.LoadBootstrap()
			if err != nil {
				return fmt.Errorf("load development identity: %w", err)
			}
			_, metadata, configErr := store.LoadConfig(bootstrap)
			_, pendingErr := os.Stat(store.Paths.Pending)
			setupPending := pendingErr == nil
			if pendingErr != nil && !errors.Is(pendingErr, os.ErrNotExist) {
				return fmt.Errorf("inspect pending development setup: %w", pendingErr)
			}

			if configErr != nil && !setupPending {
				return fmt.Errorf("load accepted development configuration: %w", configErr)
			}
			state, err := store.LoadRuntimeState()
			if err != nil {
				return fmt.Errorf("load development runtime state: %w", err)
			}

			configRevision := "unavailable"
			configDigest := "unavailable"
			if configErr == nil {
				configRevision = fmt.Sprintf("%d", metadata.Revision)
				configDigest = metadata.Digest
			}

			_, err = fmt.Fprintf(
				command.OutOrStdout(),
				"Development agent: %s\nEndpoint: %s\nState directory: %s\nGeneration: %d\nSetup complete: %t\nAccepted config revision: %s\nAccepted config digest: %s\nAuthentication paused: %t\nRevoked: %t\nRejected config revision: %d\nLast config error: %s\n",
				bootstrap.ServerID,
				bootstrap.Endpoint,
				directory,
				bootstrap.Generation,
				!setupPending && configErr == nil,
				configRevision,
				configDigest,
				state.AuthenticationPaused,
				state.Revoked,
				state.RejectedConfigRevision,
				state.LastConfigError,
			)
			return err
		},
	}
}

func developmentStore(stateDirectory string) (*agent.FileStore, string, error) {
	if strings.TrimSpace(stateDirectory) == "" {
		return nil, "", fmt.Errorf("development state directory is required")
	}
	directory, err := filepath.Abs(stateDirectory)
	if err != nil {
		return nil, "", fmt.Errorf("resolve development state directory: %w", err)
	}
	return agent.NewUserFileStore(agent.Paths{
		Identity:         filepath.Join(directory, "identity.json"),
		Pending:          filepath.Join(directory, "setup.json"),
		StandaloneConfig: filepath.Join(directory, "standalone-config.json"),
		ManagedConfig:    filepath.Join(directory, "managed-config.json"),
		State:            filepath.Join(directory, "runtime.json"),
		Commands:         filepath.Join(directory, "commands.json"),
		Logs:             filepath.Join(directory, "run-logs"),
	}), directory, nil
}

func validateDevelopmentEndpoint(value string) error {
	endpoint, err := url.Parse(value)
	if err != nil || !endpoint.IsAbs() || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return fmt.Errorf("development endpoint must be an absolute local URL")
	}
	hostname := strings.ToLower(strings.TrimSuffix(endpoint.Hostname(), "."))
	localHost := hostname == "127.0.0.1" || hostname == "::1" || hostname == "localhost" || strings.HasSuffix(hostname, ".localhost") || strings.HasSuffix(hostname, ".test")
	if !localHost {
		return fmt.Errorf("development endpoint must use localhost, a loopback address, or a .test host")
	}
	if endpoint.Scheme != "https" && !(endpoint.Scheme == "http" && hostname == "127.0.0.1") {
		return fmt.Errorf("development endpoint must use HTTPS, except for http://127.0.0.1")
	}
	if strings.TrimSuffix(endpoint.EscapedPath(), "/") != "/agent/v1" {
		return fmt.Errorf("development endpoint path must be /agent/v1")
	}
	return nil
}

func newDevelopmentHTTPClient(output io.Writer) *http.Client {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	base := &http.Transport{
		Proxy:       http.ProxyFromEnvironment,
		DialContext: dialer.DialContext,
	}
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &developmentTransport{
			base:   base,
			output: output,
		},
	}
}
