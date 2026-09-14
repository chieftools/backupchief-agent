package agent

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/chieftools/backupchief-agent/pusher"
	"github.com/chieftools/backupchief-agent/restic"
)

var ErrPermanentlyStopped = errors.New("agent execution is permanently stopped")

type RunOptions struct {
	Store          *FileStore
	Version        string
	HTTPClient     *http.Client
	HeartbeatEvery time.Duration
	ConfigEvery    time.Duration
	CommandEvery   time.Duration
	ScheduleEvery  time.Duration
	ReporterEvery  time.Duration
	DispatchEvery  time.Duration
	AllowLocal     bool
	Executor       BackupExecutor
	Now            func() time.Time
	Jitter         func(time.Duration) time.Duration
}

type daemon struct {
	store     *FileStore
	client    *Client
	bootstrap Bootstrap
	bootID    string
	now       func() time.Time

	mu       sync.Mutex
	metadata ConfigMetadata
	config   Config
	etag     string
	state    RuntimeState
	journal  CommandJournal
	executor BackupExecutor

	realtimeMu     sync.Mutex
	realtimeClient *pusher.Client
	heartbeatWake  chan struct{}
	configWake     chan struct{}
	commandWake    chan struct{}
	dispatchWake   chan struct{}
	reportWake     chan struct{}

	active             map[string]context.CancelFunc
	activeRunKinds     map[string]string
	activeWG           sync.WaitGroup
	repositories       map[string]bool
	reconciled         bool
	lastScheduleMinute time.Time
}

func Run(ctx context.Context, options RunOptions) error {
	if options.Store == nil {
		return fmt.Errorf("daemon file store is required")
	}
	bootstrap, err := options.Store.LoadBootstrap()
	if errors.Is(err, ErrManagedIdentityMissing) {
		return runStandalone(ctx, options)
	}
	if err != nil {
		return fmt.Errorf("load managed identity: %w", err)
	}
	state, err := options.Store.LoadRuntimeState()
	if err != nil {
		return fmt.Errorf("load runtime state: %w", err)
	}
	if state.Revoked {
		return fmt.Errorf("%w: setup was revoked", ErrPermanentlyStopped)
	}
	configBody, metadata, err := options.Store.LoadConfig(bootstrap)
	if errors.Is(err, ErrConfigCachePayloadInvalid) {
		if _, updateErr := UpdateConfig(ctx, ConfigUpdateOptions{
			Store: options.Store, Version: options.Version, HTTPClient: options.HTTPClient,
		}); updateErr != nil {
			return fmt.Errorf("refresh incompatible configuration: %w", updateErr)
		}
		configBody, metadata, err = options.Store.LoadConfig(bootstrap)
	}
	if err != nil {
		return fmt.Errorf("load accepted configuration: %w", err)
	}
	config, _, err := DecodeConfig(configBody, bootstrap.Generation)
	if err != nil {
		return fmt.Errorf("decode accepted configuration: %w", err)
	}
	logConfigWarnings(config)
	journal, err := options.Store.LoadCommandJournal()
	if err != nil {
		return fmt.Errorf("load command journal: %w", err)
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	bootID, err := newULID(options.Now())
	if err != nil {
		return err
	}
	if options.HeartbeatEvery == 0 {
		options.HeartbeatEvery = 5 * time.Minute
	}
	if options.ConfigEvery == 0 {
		options.ConfigEvery = 15 * time.Minute
	}
	if options.CommandEvery == 0 {
		options.CommandEvery = time.Minute
	}
	if options.ScheduleEvery == 0 {
		options.ScheduleEvery = time.Second
	}
	if options.ReporterEvery == 0 {
		options.ReporterEvery = 10 * time.Second
	}
	if options.DispatchEvery == 0 {
		options.DispatchEvery = 10 * time.Second
	}
	if options.Jitter == nil {
		options.Jitter = fullJitter
	}
	executor := options.Executor
	if executor == nil {
		executor = restic.Runner{State: options.Store.stateDirectory(), AllowLocal: options.AllowLocal}
	}
	client := NewManagedClient(bootstrap.Endpoint, bootstrap.Credential, options.Version, bootstrap.ServerID, options.HTTPClient)
	client.Now = options.Now
	runtime := &daemon{
		store:              options.Store,
		client:             client,
		bootstrap:          bootstrap,
		bootID:             bootID,
		now:                options.Now,
		metadata:           metadata,
		config:             config,
		etag:               `"` + metadata.Digest + `"`,
		state:              state,
		journal:            journal,
		executor:           executor,
		active:             map[string]context.CancelFunc{},
		activeRunKinds:     map[string]string{},
		repositories:       map[string]bool{},
		heartbeatWake:      make(chan struct{}, 1),
		configWake:         make(chan struct{}, 1),
		commandWake:        make(chan struct{}, 1),
		dispatchWake:       make(chan struct{}, 1),
		reportWake:         make(chan struct{}, 1),
		lastScheduleMinute: options.Now().UTC().Truncate(time.Minute),
	}
	client.ObserveServerTime = runtime.observeServerTime
	runtime.replaceRealtime(config.Realtime, config.ProtocolRevision)
	defer runtime.stopRealtime()

	runContext, cancel := context.WithCancel(ctx)
	defer cancel()
	errorsChannel := make(chan error, 8)
	go func() {
		errorsChannel <- runTriggeredAgentLoop(runContext, options.HeartbeatEvery, 0.10, options.Jitter, runtime.heartbeatWake, runtime.sendHeartbeat)
	}()
	go func() {
		errorsChannel <- runTriggeredAgentLoop(runContext, options.ConfigEvery, 0.10, options.Jitter, runtime.configWake, runtime.refreshConfig)
	}()
	go func() {
		errorsChannel <- runAgentLoop(runContext, time.Second, 0, options.Jitter, runtime.reloadLocalConfig)
	}()
	go func() {
		errorsChannel <- runTriggeredAgentLoop(runContext, options.CommandEvery, 0.20, options.Jitter, runtime.commandWake, runtime.processCommands)
	}()
	go func() {
		errorsChannel <- runAgentLoop(runContext, options.ScheduleEvery, 0, options.Jitter, runtime.scheduleBackups)
	}()
	go func() {
		errorsChannel <- runTriggeredAgentLoop(runContext, options.DispatchEvery, 0.20, options.Jitter, runtime.dispatchWake, runtime.dispatchCommands)
	}()
	go func() {
		errorsChannel <- runTriggeredAgentLoop(runContext, options.ReporterEvery, 0.20, options.Jitter, runtime.reportWake, runtime.reportJournal)
	}()
	go func() {
		errorsChannel <- runAgentLoop(runContext, options.ReporterEvery, 0.20, options.Jitter, runtime.reportRetiredJournal)
	}()

	var runErr error
	for completed := 0; completed < 8; completed++ {
		if err := <-errorsChannel; err != nil && runErr == nil {
			runErr = err
			cancel()
		}
	}
	runtime.activeWG.Wait()
	return runErr
}

func (daemon *daemon) reloadLocalConfig(context.Context) error {
	body, metadata, err := daemon.store.LoadConfig(daemon.bootstrap)
	if err != nil {
		log.Printf("backupchief: ignored invalid configuration update: %v", err)
		return nil
	}
	fileDigest := metadata.FileDigest
	daemon.mu.Lock()
	if fileDigest == daemon.metadata.FileDigest {
		daemon.mu.Unlock()
		return nil
	}
	daemon.mu.Unlock()
	config, _, err := DecodeConfig(body, daemon.bootstrap.Generation)
	if err != nil {
		log.Printf("backupchief: ignored invalid configuration update: %v", err)
		return nil
	}
	logConfigWarnings(config)
	daemon.mu.Lock()
	cancellations, queuedOperations := daemon.acceptMaintenanceConfigLocked(config)
	daemon.config = config
	daemon.metadata = metadata
	daemon.mu.Unlock()
	daemon.replaceRealtime(config.Realtime, config.ProtocolRevision)
	notifyLoop(daemon.heartbeatWake)
	for _, cancelRun := range cancellations {
		cancelRun()
	}
	for _, commandID := range queuedOperations {
		if err := daemon.finishWithoutExecution(commandID, "cancelled", "cancelled", "The queued operation was cancelled by the accepted job configuration."); err != nil {
			return err
		}
	}
	return nil
}

func runStandalone(ctx context.Context, options RunOptions) error {
	config, metadata, err := LoadConfiguration(options.Store.Paths.StandaloneConfig)
	if err != nil {
		return err
	}
	logConfigWarnings(config)
	if config.Host.Key != "" {
		return fmt.Errorf("managed configuration has an unusable host identity")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	executor := options.Executor
	if executor == nil {
		executor = restic.Runner{State: options.Store.stateDirectory(), AllowLocal: true}
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	lastMinute := options.Now().UTC().Truncate(time.Minute)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			updated, nextMetadata, loadErr := LoadConfiguration(options.Store.Paths.StandaloneConfig)
			if loadErr != nil {
				log.Printf("backupchief: ignored invalid configuration update: %v", loadErr)
			} else if nextMetadata.FileDigest != metadata.FileDigest {
				config, metadata = updated, nextMetadata
				log.Printf("backupchief: applied configuration update")
				logConfigWarnings(config)
			}
			minute := options.Now().UTC().Truncate(time.Minute)
			if !minute.After(lastMinute) {
				continue
			}
			lastMinute = minute
			for _, job := range config.Jobs {
				if !job.Enabled {
					continue
				}
				due, scheduleErr := scheduleIsDue(job.Schedule.Expression, minute)
				if scheduleErr != nil {
					log.Printf("backupchief: job %s has an invalid schedule: %v", job.Key, scheduleErr)
					continue
				}
				if due {
					go runStandaloneBackup(ctx, executor, options.Store.stateDirectory(), config.Host.Name, job, options.Now)
				}
			}
		}
	}
}

func runStandaloneBackup(ctx context.Context, executor BackupExecutor, stateDirectory, host string, job Job, now func() time.Time) {
	runID, err := newULID(now())
	if err != nil {
		log.Printf("backupchief: cannot create run id for job %s: %v", job.Key, err)
		return
	}
	command := &JournalCommand{RunID: runID}
	result, _, _, _ := executeBackup(ctx, executor, stateDirectory, host, 0, command, job, now)
	log.Printf("backupchief: job %s finished %s/%s: %s", job.Key, result.Status, result.ResultCode, result.Summary)
}

func (daemon *daemon) refreshConfig(ctx context.Context) error {
	daemon.mu.Lock()
	etag := daemon.etag
	previous := daemon.metadata
	daemon.mu.Unlock()

	response, err := daemon.client.FetchConfig(ctx, etag, daemon.bootstrap.Generation)
	if err != nil {
		var validationError *ConfigValidationError
		if errors.As(err, &validationError) {
			daemon.mu.Lock()
			daemon.state.RejectedConfigRevision = validationError.Response.Metadata.Revision
			daemon.state.RejectedConfigDigest = validationError.Response.Metadata.Digest
			daemon.state.LastConfigError = validationError.Error()
			if saveErr := daemon.store.SaveRuntimeState(daemon.state); saveErr != nil {
				daemon.mu.Unlock()
				return fmt.Errorf("save config rejection: %w", saveErr)
			}
			daemon.mu.Unlock()
		}
		return daemon.handleRequestError(err)
	}
	if response.NotModified {
		return daemon.clearAuthenticationPause()
	}
	metadata, err := daemon.store.SaveConfig(daemon.bootstrap, response.Body, &previous)
	if errors.Is(err, ErrConfigUnchanged) {
		return daemon.clearAuthenticationPause()
	}
	if err != nil {
		daemon.mu.Lock()
		daemon.state.LastConfigError = err.Error()
		if saveErr := daemon.store.SaveRuntimeState(daemon.state); saveErr != nil {
			daemon.mu.Unlock()
			return fmt.Errorf("save config rejection: %w", saveErr)
		}
		daemon.mu.Unlock()
		return err
	}
	config, _, err := DecodeConfig(response.Body, daemon.bootstrap.Generation)
	if err != nil {
		return err
	}
	logConfigWarnings(config)
	daemon.mu.Lock()
	cancellations, queuedOperations := daemon.acceptMaintenanceConfigLocked(config)
	daemon.metadata = metadata
	daemon.config = config
	daemon.etag = response.ETag
	daemon.state.LastConfigError = ""
	daemon.state.RejectedConfigRevision = 0
	daemon.state.RejectedConfigDigest = ""
	daemon.state.AuthenticationPaused = false
	err = daemon.store.SaveRuntimeState(daemon.state)
	daemon.mu.Unlock()
	if err != nil {
		return err
	}
	daemon.replaceRealtime(config.Realtime, config.ProtocolRevision)
	notifyLoop(daemon.heartbeatWake)
	for _, cancelRun := range cancellations {
		cancelRun()
	}
	for _, commandID := range queuedOperations {
		if finishErr := daemon.finishWithoutExecution(commandID, "cancelled", "cancelled", "The queued operation was cancelled by the accepted job configuration."); finishErr != nil {
			return finishErr
		}
	}
	return nil
}

func (daemon *daemon) sendHeartbeat(ctx context.Context) error {
	daemon.mu.Lock()
	metadata := daemon.metadata
	configState := daemon.state
	warnings := append([]string{}, daemon.config.Warnings...)
	daemon.mu.Unlock()
	status := "accepted"
	errorText := ""
	if configState.LastConfigError != "" && configState.RejectedConfigRevision > 0 && digestPattern.MatchString(configState.RejectedConfigDigest) {
		status = "rejected"
		errorText = configState.LastConfigError
		metadata.Revision = configState.RejectedConfigRevision
		metadata.Digest = configState.RejectedConfigDigest
		warnings = nil
	}
	request := HeartbeatRequest{
		Generation:         daemon.bootstrap.Generation,
		BootID:             daemon.bootID,
		SentAt:             protocolTimestamp(daemon.now()),
		ClockOffsetSeconds: configState.ClockOffsetSeconds,
		Config: HeartbeatConfig{
			ProtocolRevision: daemon.config.ProtocolRevision,
			Revision:         metadata.Revision,
			Digest:           metadata.Digest,
			Status:           status,
			Error:            errorText,
			Warnings:         warnings,
		},
		Spool: HeartbeatSpool{
			BytesUsed:   min(daemon.store.spoolUsage(), uint64(SpoolBytesLimit)),
			BytesLimit:  SpoolBytesLimit,
			GapDetected: configState.SpoolGapDetected,
		},
		ActiveRuns:   daemon.activeRunIDs(),
		Capabilities: probeCapabilities(),
	}
	if err := daemon.client.Heartbeat(ctx, request); err != nil {
		return daemon.handleRequestError(err)
	}
	return daemon.clearAuthenticationPause()
}

func logConfigWarnings(config Config) {
	for _, warning := range config.Warnings {
		log.Printf("backupchief: configuration warning: %s", warning)
	}
}

func (daemon *daemon) observeServerTime(serverTime, localTime time.Time) {
	offset := int64(serverTime.Sub(localTime).Round(time.Second) / time.Second)
	if offset > 86400 {
		offset = 86400
	} else if offset < -86400 {
		offset = -86400
	}
	daemon.mu.Lock()
	if daemon.state.ClockOffsetSeconds == offset && daemon.state.ClockOffsetObservedAt != "" {
		daemon.mu.Unlock()
		return
	}
	daemon.state.ClockOffsetSeconds = offset
	daemon.state.ClockOffsetObservedAt = protocolTimestamp(localTime)
	_ = daemon.store.SaveRuntimeState(daemon.state)
	daemon.mu.Unlock()
}

func (daemon *daemon) activeRunIDs() []string {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	ids := make([]string, 0, len(daemon.active))
	for id := range daemon.active {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (daemon *daemon) handleRequestError(err error) error {
	var apiError *APIError
	if !errors.As(err, &apiError) {
		return err
	}
	switch apiError.Code {
	case "enrollment_revoked":
		daemon.mu.Lock()
		daemon.state.Revoked = true
		if saveErr := daemon.store.SaveRuntimeState(daemon.state); saveErr != nil {
			daemon.mu.Unlock()
			return fmt.Errorf("persist revoked state: %w", saveErr)
		}
		for _, cancel := range daemon.active {
			cancel()
		}
		daemon.mu.Unlock()
		return fmt.Errorf("%w: setup was revoked", ErrPermanentlyStopped)
	case "invalid_authentication":
		daemon.mu.Lock()
		daemon.state.AuthenticationPaused = true
		if saveErr := daemon.store.SaveRuntimeState(daemon.state); saveErr != nil {
			daemon.mu.Unlock()
			return fmt.Errorf("persist authentication pause: %w", saveErr)
		}
		daemon.mu.Unlock()
	}
	return err
}

func (daemon *daemon) clearAuthenticationPause() error {
	daemon.mu.Lock()
	if !daemon.state.AuthenticationPaused {
		daemon.mu.Unlock()
		return nil
	}
	daemon.state.AuthenticationPaused = false
	err := daemon.store.SaveRuntimeState(daemon.state)
	daemon.mu.Unlock()
	return err
}

func (daemon *daemon) replaceRealtime(config *pusher.Config, protocolRevision string) {
	var next *pusher.Client
	if config != nil {
		copied := *config
		next = pusher.NewClient(&copied, daemon.client.userAgent(), daemon.bootstrap.Credential, protocolRevision, daemon.client.HTTP, false)
		next.OnEvent("config.updated", func(pusher.Message) {
			notifyLoop(daemon.configWake)
		})
		next.OnEvent("commands.available", func(pusher.Message) {
			notifyLoop(daemon.commandWake)
		})
		next.OnEvent("pusher_internal:subscription_succeeded", func(pusher.Message) {
			notifyLoop(daemon.configWake)
			notifyLoop(daemon.commandWake)
		})
	}

	daemon.realtimeMu.Lock()
	previous := daemon.realtimeClient
	daemon.realtimeClient = next
	daemon.realtimeMu.Unlock()
	if previous != nil {
		previous.Disconnect()
	}
	if next != nil {
		go next.Start()
	}
}

func (daemon *daemon) stopRealtime() {
	daemon.realtimeMu.Lock()
	client := daemon.realtimeClient
	daemon.realtimeClient = nil
	daemon.realtimeMu.Unlock()
	if client != nil {
		client.Disconnect()
	}
}

func notifyLoop(wake chan<- struct{}) {
	select {
	case wake <- struct{}{}:
	default:
	}
}

func runAgentLoop(ctx context.Context, normalInterval time.Duration, jitterFraction float64, jitter func(time.Duration) time.Duration, action func(context.Context) error) error {
	return runTriggeredAgentLoop(ctx, normalInterval, jitterFraction, jitter, nil, action)
}

func runTriggeredAgentLoop(ctx context.Context, normalInterval time.Duration, jitterFraction float64, jitter func(time.Duration) time.Duration, wake <-chan struct{}, action func(context.Context) error) error {
	backoff := 2 * time.Second
	for {
		err := action(ctx)
		if errors.Is(err, ErrPermanentlyStopped) {
			return err
		}
		wait := normalJitter(normalInterval, jitterFraction, jitter)
		if err != nil {
			var apiError *APIError
			if errors.As(err, &apiError) && apiError.RetryAfter > 0 {
				wait = apiError.RetryAfter
			} else {
				wait = jitter(backoff)
			}
			backoff *= 2
			if backoff > 5*time.Minute {
				backoff = 5 * time.Minute
			}
		} else {
			backoff = 2 * time.Second
		}
		activeWake := wake
		if err != nil {
			activeWake = nil
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		case <-activeWake:
			if !timer.Stop() {
				<-timer.C
			}
		}
	}
}

func normalJitter(interval time.Duration, fraction float64, jitter func(time.Duration) time.Duration) time.Duration {
	window := time.Duration(float64(interval) * fraction)
	if window == 0 {
		return interval
	}
	return interval - window + jitter(2*window)
}

func fullJitter(maximum time.Duration) time.Duration {
	if maximum <= 0 {
		return 0
	}
	value, err := rand.Int(rand.Reader, big.NewInt(int64(maximum)+1))
	if err != nil {
		return maximum
	}
	return time.Duration(value.Int64())
}
