package agent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

type ConfigUpdateOptions struct {
	Store      *FileStore
	Version    string
	HTTPClient *http.Client
	Force      bool
}

type ConfigUpdateResult struct {
	Metadata ConfigMetadata
	Updated  bool
}

func UpdateConfig(ctx context.Context, options ConfigUpdateOptions) (ConfigUpdateResult, error) {
	if options.Store == nil {
		return ConfigUpdateResult{}, fmt.Errorf("configuration file store is required")
	}

	bootstrap, err := options.Store.LoadBootstrap()
	if err != nil {
		return ConfigUpdateResult{}, fmt.Errorf("load managed identity: %w", err)
	}
	state, err := options.Store.LoadRuntimeState()
	if err != nil {
		return ConfigUpdateResult{}, fmt.Errorf("load runtime state: %w", err)
	}
	if state.Revoked {
		return ConfigUpdateResult{}, fmt.Errorf("%w: enrollment was revoked", ErrPermanentlyStopped)
	}

	_, current, cacheErr := options.Store.LoadConfig(bootstrap)
	var previous *ConfigMetadata
	if (cacheErr == nil || errors.Is(cacheErr, ErrConfigCachePayloadInvalid)) &&
		current.Generation == bootstrap.Generation && current.Revision > 0 && digestPattern.MatchString(current.Digest) {
		previous = &current
	}

	etag := ""
	if !options.Force && cacheErr == nil {
		etag = `"` + current.Digest + `"`
	}

	client := NewClient(bootstrap.Endpoint, bootstrap.Credential, options.Version, options.HTTPClient)
	response, err := client.FetchConfig(ctx, etag, bootstrap.Generation)
	if err != nil {
		return ConfigUpdateResult{}, err
	}
	if response.NotModified {
		if cacheErr != nil {
			return ConfigUpdateResult{}, fmt.Errorf("control plane returned not modified for an unusable cached configuration")
		}
		return ConfigUpdateResult{Metadata: current}, nil
	}

	metadata, err := options.Store.SaveConfig(bootstrap, response.Body, previous)
	if errors.Is(err, ErrConfigUnchanged) {
		return ConfigUpdateResult{Metadata: metadata}, nil
	}
	if err != nil {
		return ConfigUpdateResult{}, fmt.Errorf("save configuration: %w", err)
	}

	return ConfigUpdateResult{Metadata: metadata, Updated: true}, nil
}
