package agent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

type ServiceManager interface {
	Stop(context.Context) error
	EnableAndStart(context.Context) error
}

type SystemdServiceManager struct{}

func (SystemdServiceManager) Stop(ctx context.Context) error {
	command := exec.CommandContext(ctx, "systemctl", "stop", "backupchief.service")
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("stop backupchief.service: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (SystemdServiceManager) EnableAndStart(ctx context.Context) error {
	command := exec.CommandContext(ctx, "systemctl", "enable", "--now", "backupchief.service")
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("enable and start backupchief.service: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

type EnrollOptions struct {
	Store          *FileStore
	Endpoint       string
	Version        string
	Hostname       string
	Platform       string
	Architecture   string
	HTTPClient     *http.Client
	ServiceManager ServiceManager
	RootCheck      func() bool
	Now            func() time.Time
}

func Enroll(ctx context.Context, token string, options EnrollOptions) (Bootstrap, error) {
	if options.RootCheck == nil {
		options.RootCheck = func() bool { return os.Geteuid() == 0 }
	}
	if !options.RootCheck() {
		return Bootstrap{}, fmt.Errorf("setup must run as root")
	}
	if options.Store == nil {
		return Bootstrap{}, fmt.Errorf("setup file store is required")
	}
	if options.Endpoint == "" {
		options.Endpoint = DefaultEndpoint
	}
	if !validControlPlaneEndpoint(options.Endpoint) {
		return Bootstrap{}, fmt.Errorf("control-plane endpoint must use HTTPS")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Hostname == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return Bootstrap{}, fmt.Errorf("read hostname: %w", err)
		}
		options.Hostname = hostname
	}
	if options.Platform == "" || options.Architecture == "" {
		options.Platform, options.Architecture = platformValues()
	}
	if options.Platform != "linux" || options.Architecture != "amd64" && options.Architecture != "arm64" {
		return Bootstrap{}, fmt.Errorf("setup requires Linux amd64 or arm64")
	}
	if token == "" {
		return Bootstrap{}, fmt.Errorf("setup token is required")
	}
	if options.ServiceManager == nil {
		options.ServiceManager = SystemdServiceManager{}
	}

	tokenHash := tokenDigest(token)
	bootstrap, bootstrapErr := options.Store.LoadBootstrap()
	if bootstrapErr == nil && bootstrap.SetupTokenHash == tokenHash {
		if err := ensureUsableConfig(ctx, options, bootstrap); err != nil {
			return Bootstrap{}, err
		}
		if err := options.Store.SaveRuntimeState(RuntimeState{}); err != nil {
			return Bootstrap{}, fmt.Errorf("reset runtime state: %w", err)
		}
		if err := options.Store.RemovePending(); err != nil {
			return Bootstrap{}, fmt.Errorf("remove pending setup: %w", err)
		}
		if err := options.ServiceManager.EnableAndStart(ctx); err != nil {
			return Bootstrap{}, err
		}
		return bootstrap, nil
	}
	if bootstrapErr != nil && !errors.Is(bootstrapErr, ErrManagedIdentityMissing) {
		return Bootstrap{}, fmt.Errorf("load installed identity: %w", bootstrapErr)
	}
	reenrolling := bootstrapErr == nil

	pending, err := options.Store.LoadPending()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return Bootstrap{}, fmt.Errorf("load pending setup: %w", err)
	}
	if errors.Is(err, os.ErrNotExist) || pending.TokenHash != tokenHash || pending.Endpoint != options.Endpoint {
		attemptID, createErr := newULID(options.Now())
		if createErr != nil {
			return Bootstrap{}, createErr
		}
		credential, createErr := randomSecret()
		if createErr != nil {
			return Bootstrap{}, createErr
		}
		pending = pendingEnrollment{
			Endpoint:   options.Endpoint,
			TokenHash:  tokenHash,
			AttemptID:  attemptID,
			Credential: credential,
		}
		if reenrolling {
			pending.PreviousServerID = bootstrap.ServerID
			pending.PreviousGeneration = bootstrap.Generation
		}
		if err := options.Store.SavePending(pending); err != nil {
			return Bootstrap{}, fmt.Errorf("save pending setup: %w", err)
		}
	}
	if reenrolling {
		if pending.PreviousServerID != bootstrap.ServerID || pending.PreviousGeneration != bootstrap.Generation {
			return Bootstrap{}, fmt.Errorf("pending setup does not match the installed identity")
		}
		if err := options.ServiceManager.Stop(ctx); err != nil {
			return Bootstrap{}, err
		}
		if err := prepareRetiredGeneration(options.Store, bootstrap, options.Now()); err != nil {
			return Bootstrap{}, fmt.Errorf("prepare retired setup: %w", err)
		}
	}

	client := NewClient(options.Endpoint, "", options.Version, options.HTTPClient)
	if reenrolling {
		client = NewManagedClient(options.Endpoint, "", options.Version, bootstrap.ServerID, options.HTTPClient)
	}
	response, err := client.Enroll(ctx, token, EnrollmentRequest{
		ProtocolRevision: ProtocolRevision,
		AttemptID:        pending.AttemptID,
		AgentVersion:     normalizeVersion(options.Version),
		Hostname:         options.Hostname,
		Platform:         options.Platform,
		Architecture:     options.Architecture,
		Credential:       pending.Credential,
	})
	if err != nil {
		return Bootstrap{}, err
	}
	if reenrolling && (response.ServerID != bootstrap.ServerID || response.Generation != bootstrap.Generation+1) {
		return Bootstrap{}, fmt.Errorf("setup response does not advance the installed server generation")
	}
	if !reenrolling {
		if err := options.ServiceManager.Stop(ctx); err != nil {
			return Bootstrap{}, err
		}
	}
	bootstrap = Bootstrap{
		Endpoint:       options.Endpoint,
		ServerID:       response.ServerID,
		Generation:     response.Generation,
		Credential:     pending.Credential,
		Hostname:       options.Hostname,
		SetupTokenHash: tokenHash,
	}
	if err := options.Store.SaveBootstrap(bootstrap); err != nil {
		return Bootstrap{}, fmt.Errorf("save managed identity: %w", err)
	}
	if err := ensureUsableConfig(ctx, options, bootstrap); err != nil {
		return Bootstrap{}, err
	}
	if err := options.Store.SaveRuntimeState(RuntimeState{}); err != nil {
		return Bootstrap{}, fmt.Errorf("reset runtime state: %w", err)
	}
	if err := options.Store.RemovePending(); err != nil {
		return Bootstrap{}, fmt.Errorf("remove pending setup: %w", err)
	}
	if err := options.ServiceManager.EnableAndStart(ctx); err != nil {
		return Bootstrap{}, err
	}
	return bootstrap, nil
}

func ensureUsableConfig(ctx context.Context, options EnrollOptions, bootstrap Bootstrap) error {
	if _, _, err := options.Store.LoadConfig(bootstrap); err == nil {
		return nil
	}
	client := NewManagedClient(bootstrap.Endpoint, bootstrap.Credential, options.Version, bootstrap.ServerID, options.HTTPClient)
	response, err := client.FetchConfig(ctx, "", bootstrap.Generation)
	if err != nil {
		return err
	}
	if response.NotModified {
		return fmt.Errorf("control plane returned not modified without a usable cache")
	}
	if response.Metadata.Revision != 1 {
		return fmt.Errorf("initial configuration must be revision 1")
	}
	if _, err := options.Store.SaveConfig(bootstrap, response.Body, nil); err != nil {
		return fmt.Errorf("save initial configuration: %w", err)
	}
	return nil
}
