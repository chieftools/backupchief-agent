package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

type APIError struct {
	Status     int
	Code       string
	Detail     string
	RetryAfter time.Duration
}

func (err *APIError) Error() string {
	if err.Detail != "" {
		return err.Detail
	}
	return err.Code
}

type problemResponse struct {
	Type              string `json:"type"`
	Title             string `json:"title"`
	Status            int    `json:"status"`
	Code              string `json:"code"`
	Detail            string `json:"detail,omitempty"`
	RetryAfterSeconds int    `json:"retry_after_seconds,omitempty"`
}

type ConfigResponse struct {
	Body        []byte
	Metadata    ConfigMetadata
	ETag        string
	NotModified bool
}

type ConfigValidationError struct {
	Response ConfigResponse
	Err      error
}

type ManagedBackupStatus struct {
	ID      string `json:"id"`
	Status  string `json:"status"`
	Summary string `json:"summary,omitempty"`
}

func (err *ConfigValidationError) Error() string {
	return err.Err.Error()
}

func (err *ConfigValidationError) Unwrap() error {
	return err.Err
}

type Client struct {
	Endpoint          string
	Credential        string
	HTTP              *http.Client
	Version           string
	Now               func() time.Time
	ObserveServerTime func(time.Time, time.Time)
}

func NewClient(endpoint, credential, version string, httpClient *http.Client) *Client {
	if httpClient == nil {
		dialer := &net.Dialer{Timeout: 5 * time.Second}
		httpClient = &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				Proxy:       http.ProxyFromEnvironment,
				DialContext: dialer.DialContext,
			},
		}
	}
	return &Client{
		Endpoint:   strings.TrimRight(endpoint, "/"),
		Credential: credential,
		HTTP:       httpClient,
		Version:    normalizeVersion(version),
		Now:        time.Now,
	}
}

func (client *Client) Enroll(ctx context.Context, token string, request EnrollmentRequest) (EnrollmentResponse, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return EnrollmentResponse{}, fmt.Errorf("encode enrollment request: %w", err)
	}
	response, err := client.request(ctx, http.MethodPost, "/enroll", token, "", body)
	if err != nil {
		return EnrollmentResponse{}, err
	}
	defer response.Body.Close()
	if err := requireProtocolResponse(response); err != nil {
		return EnrollmentResponse{}, err
	}
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		return EnrollmentResponse{}, decodeProblem(response)
	}
	data, err := readLimited(response.Body, 64<<10)
	if err != nil {
		return EnrollmentResponse{}, fmt.Errorf("read enrollment response: %w", err)
	}
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return EnrollmentResponse{}, fmt.Errorf("validate enrollment response: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var enrollment EnrollmentResponse
	if err := decoder.Decode(&enrollment); err != nil {
		return EnrollmentResponse{}, fmt.Errorf("decode enrollment response: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return EnrollmentResponse{}, fmt.Errorf("decode enrollment response: %w", err)
	}
	if enrollment.ProtocolRevision != ProtocolRevision || !ulidPattern.MatchString(enrollment.ServerID) || enrollment.Generation == 0 || enrollment.ConfigRevision == 0 || !validateTimestamp(enrollment.EnrolledAt) {
		return EnrollmentResponse{}, fmt.Errorf("enrollment response is incompatible")
	}
	return enrollment, nil
}

func (client *Client) FetchConfig(ctx context.Context, etag string, expectedGeneration uint64) (ConfigResponse, error) {
	response, err := client.request(ctx, http.MethodGet, "/config", client.Credential, etag, nil)
	if err != nil {
		return ConfigResponse{}, err
	}
	defer response.Body.Close()
	if err := requireProtocolResponse(response); err != nil {
		return ConfigResponse{}, err
	}
	if response.StatusCode == http.StatusNotModified {
		return ConfigResponse{ETag: etag, NotModified: true}, nil
	}
	if response.StatusCode != http.StatusOK {
		return ConfigResponse{}, decodeProblem(response)
	}
	body, err := readLimited(response.Body, maximumConfig)
	if err != nil {
		return ConfigResponse{}, fmt.Errorf("read configuration: %w", err)
	}
	responseETag := response.Header.Get("ETag")
	_, metadata, err := DecodeConfig(body, expectedGeneration)
	if metadata.Digest != metadata.FileDigest {
		return ConfigResponse{Body: body, Metadata: metadata}, fmt.Errorf("configuration response contains an installed-cache digest")
	}
	if err != nil {
		configResponse := ConfigResponse{Body: body, Metadata: metadata, ETag: responseETag}
		if metadata.Revision > 0 && responseETag == `"`+metadata.FileDigest+`"` {
			return configResponse, &ConfigValidationError{Response: configResponse, Err: err}
		}
		return configResponse, err
	}
	if responseETag != `"`+metadata.FileDigest+`"` {
		return ConfigResponse{Body: body, Metadata: metadata}, fmt.Errorf("configuration ETag does not match its body")
	}
	return ConfigResponse{Body: body, Metadata: metadata, ETag: responseETag}, nil
}

func (client *Client) StartManagedBackup(ctx context.Context, jobID string) (ManagedBackupStatus, error) {
	body, err := json.Marshal(map[string]string{"job_id": jobID})
	if err != nil {
		return ManagedBackupStatus{}, err
	}
	response, err := client.request(ctx, http.MethodPost, "/backups", client.Credential, "", body)
	if err != nil {
		return ManagedBackupStatus{}, err
	}
	defer response.Body.Close()
	if err := requireProtocolResponse(response); err != nil {
		return ManagedBackupStatus{}, err
	}
	if response.StatusCode != http.StatusAccepted && response.StatusCode != http.StatusOK {
		return ManagedBackupStatus{}, decodeProblem(response)
	}
	return decodeManagedBackupStatus(response.Body)
}

func (client *Client) WaitForManagedBackup(ctx context.Context, commandID string) (ManagedBackupStatus, error) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		status, err := client.managedBackupStatus(ctx, commandID)
		if err != nil {
			if ctx.Err() != nil {
				cancelContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = client.cancelManagedBackup(cancelContext, commandID)
			}
			return ManagedBackupStatus{}, err
		}
		if slices.Contains([]string{"complete", "partial", "failed", "cancelled", "skipped", "unresolved", "expired"}, status.Status) {
			return status, nil
		}
		select {
		case <-ctx.Done():
			cancelContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = client.cancelManagedBackup(cancelContext, commandID)
			cancel()
			return ManagedBackupStatus{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (client *Client) managedBackupStatus(ctx context.Context, commandID string) (ManagedBackupStatus, error) {
	response, err := client.request(ctx, http.MethodGet, "/backups/"+commandID, client.Credential, "", nil)
	if err != nil {
		return ManagedBackupStatus{}, err
	}
	defer response.Body.Close()
	if err := requireProtocolResponse(response); err != nil {
		return ManagedBackupStatus{}, err
	}
	if response.StatusCode != http.StatusOK {
		return ManagedBackupStatus{}, decodeProblem(response)
	}
	return decodeManagedBackupStatus(response.Body)
}

func (client *Client) cancelManagedBackup(ctx context.Context, commandID string) error {
	response, err := client.request(ctx, http.MethodPost, "/backups/"+commandID+"/cancel", client.Credential, "", []byte("{}"))
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if err := requireProtocolResponse(response); err != nil {
		return err
	}
	if response.StatusCode != http.StatusNoContent {
		return decodeProblem(response)
	}
	return nil
}

func decodeManagedBackupStatus(reader io.Reader) (ManagedBackupStatus, error) {
	data, err := readLimited(reader, 64<<10)
	if err != nil {
		return ManagedBackupStatus{}, err
	}
	var status ManagedBackupStatus
	if err := decodeStrict(data, &status); err != nil {
		return ManagedBackupStatus{}, fmt.Errorf("decode backup status: %w", err)
	}
	if !ulidPattern.MatchString(status.ID) || !slices.Contains([]string{"pending", "acknowledged", "running", "complete", "partial", "failed", "cancelled", "skipped", "unresolved", "expired"}, status.Status) {
		return ManagedBackupStatus{}, fmt.Errorf("backup status is incompatible")
	}
	return status, nil
}

func (client *Client) Heartbeat(ctx context.Context, request HeartbeatRequest) error {
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode heartbeat: %w", err)
	}
	if len(body) > 64<<10 {
		return fmt.Errorf("heartbeat exceeds 64 KiB")
	}
	response, err := client.request(ctx, http.MethodPost, "/heartbeat", client.Credential, "", body)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if err := requireProtocolResponse(response); err != nil {
		return err
	}
	if response.StatusCode != http.StatusNoContent {
		return decodeProblem(response)
	}
	return nil
}

func (client *Client) PollCommands(ctx context.Context, expectedGeneration uint64) (CommandsResponse, error) {
	response, err := client.request(ctx, http.MethodGet, "/commands", client.Credential, "", nil)
	if err != nil {
		return CommandsResponse{}, err
	}
	defer response.Body.Close()
	if err := requireProtocolResponse(response); err != nil {
		return CommandsResponse{}, err
	}
	if response.StatusCode != http.StatusOK {
		return CommandsResponse{}, decodeProblem(response)
	}
	body, err := readLimited(response.Body, maximumCommands)
	if err != nil {
		return CommandsResponse{}, fmt.Errorf("read commands: %w", err)
	}
	var commands CommandsResponse
	if err := decodeStrict(body, &commands); err != nil {
		return CommandsResponse{}, fmt.Errorf("decode commands: %w", err)
	}
	if commands.ProtocolRevision != ProtocolRevision || commands.Commands == nil || len(commands.Commands) > 25 {
		return CommandsResponse{}, fmt.Errorf("commands response is incompatible")
	}
	for _, command := range commands.Commands {
		if err := validateCommand(command, expectedGeneration); err != nil {
			return CommandsResponse{}, err
		}
	}
	return commands, nil
}

func (client *Client) AcknowledgeCommand(ctx context.Context, commandID string, acknowledgement CommandAcknowledgement) error {
	return client.postJSON(ctx, "/commands/"+commandID+"/ack", acknowledgement)
}

func (client *Client) SubmitResult(ctx context.Context, commandID string, result CommandResult) error {
	return client.postJSON(ctx, "/commands/"+commandID+"/result", result)
}

func (client *Client) SubmitEvents(ctx context.Context, request EventRequest) (EventsResponse, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return EventsResponse{}, fmt.Errorf("encode events: %w", err)
	}
	if len(body) > 1<<20 {
		return EventsResponse{}, fmt.Errorf("events exceed 1 MiB")
	}
	response, err := client.request(ctx, http.MethodPost, "/events", client.Credential, "", body)
	if err != nil {
		return EventsResponse{}, err
	}
	defer response.Body.Close()
	if err := requireProtocolResponse(response); err != nil {
		return EventsResponse{}, err
	}
	if response.StatusCode != http.StatusOK {
		return EventsResponse{}, decodeProblem(response)
	}
	data, err := readLimited(response.Body, 64<<10)
	if err != nil {
		return EventsResponse{}, fmt.Errorf("read event response: %w", err)
	}
	var result EventsResponse
	if err := decodeStrict(data, &result); err != nil {
		return EventsResponse{}, fmt.Errorf("decode event response: %w", err)
	}
	if result.ProtocolRevision != ProtocolRevision || len(result.Results) != len(request.Events) {
		return EventsResponse{}, fmt.Errorf("event response does not match request")
	}
	for index, item := range result.Results {
		if item.ID != request.Events[index].ID || !slices.Contains([]string{"accepted", "duplicate", "rejected"}, item.Status) {
			return EventsResponse{}, fmt.Errorf("event response item is invalid")
		}
	}
	return result, nil
}

func (client *Client) UploadLogChunk(ctx context.Context, runID, logID string, index, offset int, body []byte) error {
	if len(body) > maximumLogChunk {
		return fmt.Errorf("log chunk exceeds 256 KiB")
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/octet-stream")
	headers.Set("BackupChief-Chunk-Offset", strconv.Itoa(offset))
	headers.Set("BackupChief-Chunk-SHA256", digestBody(body))
	response, err := client.requestContent(
		ctx,
		http.MethodPut,
		fmt.Sprintf("/runs/%s/logs/%s/chunks/%d", runID, logID, index),
		client.Credential,
		body,
		headers,
	)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if err := requireProtocolResponse(response); err != nil {
		return err
	}
	if response.StatusCode != http.StatusNoContent {
		return decodeProblem(response)
	}
	return nil
}

func (client *Client) CompleteLog(ctx context.Context, runID, logID string, completion LogCompletion) error {
	return client.postJSON(ctx, "/runs/"+runID+"/logs/"+logID+"/complete", completion)
}

func (client *Client) postJSON(ctx context.Context, path string, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	if len(body) > 64<<10 {
		return fmt.Errorf("request exceeds 64 KiB")
	}
	response, err := client.request(ctx, http.MethodPost, path, client.Credential, "", body)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if err := requireProtocolResponse(response); err != nil {
		return err
	}
	if response.StatusCode != http.StatusNoContent {
		return decodeProblem(response)
	}
	return nil
}

func (client *Client) request(ctx context.Context, method, path, credential, etag string, body []byte) (*http.Response, error) {
	headers := http.Header{}
	if body != nil {
		headers.Set("Content-Type", "application/json")
	}
	if etag != "" {
		headers.Set("If-None-Match", etag)
	}
	return client.requestContent(ctx, method, path, credential, body, headers)
}

func (client *Client) requestContent(ctx context.Context, method, path, credential string, body []byte, headers http.Header) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, client.Endpoint+path, reader)
	if err != nil {
		return nil, fmt.Errorf("create control-plane request: %w", err)
	}
	request.Header.Set(ProtocolHeader, ProtocolRevision)
	request.Header.Set("User-Agent", "backupchief/"+client.Version)
	request.Header.Set("Accept", "application/json, application/problem+json")
	request.Header.Set("Accept-Encoding", "identity")
	for name, values := range headers {
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}
	if credential != "" {
		request.Header.Set("Authorization", "Bearer "+credential)
	}
	startedAt := client.Now()
	response, err := client.HTTP.Do(request)
	if err != nil {
		return nil, fmt.Errorf("contact control plane: %w", err)
	}
	if serverTime, parseErr := http.ParseTime(response.Header.Get("Date")); parseErr == nil && client.ObserveServerTime != nil {
		finishedAt := client.Now()
		client.ObserveServerTime(serverTime, startedAt.Add(finishedAt.Sub(startedAt)/2))
	}
	return response, nil
}

func decodeStrict(data []byte, target any) error {
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

func validateCommand(command AgentCommand, expectedGeneration uint64) error {
	if !ulidPattern.MatchString(command.ID) || command.Generation != expectedGeneration || !validateTimestamp(command.IssuedAt) || !validateTimestamp(command.ExpiresAt) {
		return fmt.Errorf("command identity is invalid")
	}
	issued, _ := time.Parse("2006-01-02T15:04:05.000000Z", command.IssuedAt)
	expires, _ := time.Parse("2006-01-02T15:04:05.000000Z", command.ExpiresAt)
	if !expires.After(issued) || expires.Sub(issued) > 24*time.Hour {
		return fmt.Errorf("command expiry is invalid")
	}
	switch command.Kind {
	case "run_backup":
		if !ulidPattern.MatchString(command.Payload.JobID) || command.Payload.RequiredConfigRevision == 0 || command.Payload.RunID != "" || command.Payload.Maintenance != "" {
			return fmt.Errorf("backup command payload is invalid")
		}
	case "run_maintenance":
		if !ulidPattern.MatchString(command.Payload.JobID) || command.Payload.RequiredConfigRevision == 0 || command.Payload.RunID != "" || !contains([]string{"forget", "prune", "check_metadata", "check_data"}, command.Payload.Maintenance) {
			return fmt.Errorf("maintenance command payload is invalid")
		}
	case "cancel_run":
		if !ulidPattern.MatchString(command.Payload.JobID) || !ulidPattern.MatchString(command.Payload.RunID) || command.Payload.RequiredConfigRevision != 0 || command.Payload.Maintenance != "" {
			return fmt.Errorf("cancellation command payload is invalid")
		}
	default:
		return fmt.Errorf("unsupported command kind %q", command.Kind)
	}
	return nil
}

func requireProtocolResponse(response *http.Response) error {
	if response.Header.Get(ProtocolHeader) != ProtocolRevision {
		return fmt.Errorf("control plane returned an unsupported protocol revision")
	}
	return nil
}

func decodeProblem(response *http.Response) error {
	body, err := readLimited(response.Body, 64<<10)
	if err != nil {
		return fmt.Errorf("read control-plane problem: %w", err)
	}
	if err := rejectDuplicateJSONKeys(body); err != nil {
		return fmt.Errorf("validate control-plane problem: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var problem problemResponse
	if err := decoder.Decode(&problem); err != nil {
		return fmt.Errorf("decode control-plane problem: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return fmt.Errorf("decode control-plane problem: %w", err)
	}
	if problem.Status != response.StatusCode || problem.Code == "" || problem.Title == "" || problem.Type == "" {
		return fmt.Errorf("control plane returned an invalid problem response")
	}
	retryAfter := problem.RetryAfterSeconds
	if header := response.Header.Get("Retry-After"); header != "" {
		if seconds, parseErr := strconv.Atoi(header); parseErr == nil {
			retryAfter = seconds
		}
	}
	return &APIError{
		Status:     response.StatusCode,
		Code:       problem.Code,
		Detail:     problem.Detail,
		RetryAfter: time.Duration(retryAfter) * time.Second,
	}
}

func readLimited(reader io.Reader, maximum int) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, int64(maximum)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maximum {
		return nil, errors.New("response exceeds its size limit")
	}
	return data, nil
}

func digestBody(body []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(body))
}
