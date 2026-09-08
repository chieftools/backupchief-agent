package agent

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"math/big"
	"net/url"
	"regexp"
	"runtime"
	"time"
)

const (
	ProtocolRevision = "1.0.0"
	ProtocolHeader   = "BackupChief-Protocol-Revision"
	DefaultEndpoint  = "https://backup.chief.app/agent/v1"
	SpoolBytesLimit  = 268435456
	maximumConfig    = 1 << 20
	maximumCommands  = 512 << 10
	maximumRunLog    = 8 << 20
	maximumLogChunk  = 256 << 10
)

var (
	ulidPattern      = regexp.MustCompile(`^[0-9a-hjkmnp-tv-z]{26}$`)
	digestPattern    = regexp.MustCompile(`^[a-f0-9]{64}$`)
	versionPattern   = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?$`)
	timestampPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$`)
)

type Bootstrap struct {
	Endpoint            string `json:"endpoint"`
	ServerID            string `json:"server_id"`
	Generation          uint64 `json:"generation"`
	Credential          string `json:"credential"`
	Hostname            string `json:"hostname,omitempty"`
	SetupTokenHash      string `json:"setup_token_hash,omitempty"`
	CacheKey            string `json:"cache_key,omitempty"`
	EnrollmentTokenHash string `json:"enrollment_token_hash,omitempty"`
}

type pendingEnrollment struct {
	Endpoint           string `json:"endpoint"`
	TokenHash          string `json:"token_hash"`
	AttemptID          string `json:"attempt_id"`
	Credential         string `json:"credential"`
	CacheKey           string `json:"cache_key,omitempty"`
	PreviousServerID   string `json:"previous_server_id,omitempty"`
	PreviousGeneration uint64 `json:"previous_generation,omitempty"`
}

type ConfigMetadata struct {
	Generation uint64
	Revision   uint64
	Digest     string
	FileDigest string
}

type RuntimeState struct {
	Revoked                bool                          `json:"revoked"`
	AuthenticationPaused   bool                          `json:"authentication_paused"`
	RejectedConfigRevision uint64                        `json:"rejected_config_revision,omitempty"`
	RejectedConfigDigest   string                        `json:"rejected_config_digest,omitempty"`
	LastConfigError        string                        `json:"last_config_error,omitempty"`
	SpoolGapDetected       bool                          `json:"spool_gap_detected,omitempty"`
	ClockOffsetSeconds     int64                         `json:"clock_offset_seconds,omitempty"`
	ClockOffsetObservedAt  string                        `json:"clock_offset_observed_at,omitempty"`
	Maintenance            map[string]MaintenanceRuntime `json:"maintenance,omitempty"`
}

type CompleteSnapshotProof struct {
	RunID       string   `json:"run_id"`
	FinishedAt  string   `json:"finished_at"`
	SnapshotIDs []string `json:"snapshot_ids"`
}

type MaintenanceRuntime struct {
	LatestComplete       *CompleteSnapshotProof `json:"latest_complete,omitempty"`
	Unresolved           bool                   `json:"unresolved,omitempty"`
	UnresolvedConfirmed  bool                   `json:"unresolved_confirmed,omitempty"`
	UnresolvedAtRevision uint64                 `json:"unresolved_at_revision,omitempty"`
	DataParts            uint64                 `json:"data_parts,omitempty"`
	NextDataPart         uint64                 `json:"next_data_part,omitempty"`
}

type EnrollmentRequest struct {
	ProtocolRevision string `json:"protocol_revision"`
	AttemptID        string `json:"attempt_id"`
	AgentVersion     string `json:"agent_version"`
	Hostname         string `json:"hostname"`
	Platform         string `json:"platform"`
	Architecture     string `json:"architecture"`
	Credential       string `json:"credential"`
}

type EnrollmentResponse struct {
	ProtocolRevision string `json:"protocol_revision"`
	ServerID         string `json:"server_id"`
	Generation       uint64 `json:"generation"`
	EnrolledAt       string `json:"enrolled_at"`
	ConfigRevision   uint64 `json:"config_revision"`
}

type HeartbeatRequest struct {
	Generation         uint64          `json:"generation"`
	BootID             string          `json:"boot_id"`
	SentAt             string          `json:"sent_at"`
	ClockOffsetSeconds int64           `json:"clock_offset_seconds,omitempty"`
	Config             HeartbeatConfig `json:"config"`
	Spool              HeartbeatSpool  `json:"spool"`
	ActiveRuns         []string        `json:"active_runs"`
}

type HeartbeatConfig struct {
	Revision uint64   `json:"revision"`
	Digest   string   `json:"digest"`
	Status   string   `json:"status"`
	Error    string   `json:"error,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

type HeartbeatSpool struct {
	BytesUsed   uint64 `json:"bytes_used"`
	BytesLimit  uint64 `json:"bytes_limit"`
	GapDetected bool   `json:"gap_detected"`
}

type CommandsResponse struct {
	ProtocolRevision string         `json:"protocol_revision"`
	Commands         []AgentCommand `json:"commands"`
}

type AgentCommand struct {
	ID         string         `json:"id"`
	Generation uint64         `json:"generation"`
	Kind       string         `json:"kind"`
	IssuedAt   string         `json:"issued_at"`
	ExpiresAt  string         `json:"expires_at"`
	Payload    CommandPayload `json:"payload"`
}

type CommandPayload struct {
	JobID                  string `json:"job_id,omitempty"`
	RunID                  string `json:"run_id,omitempty"`
	RequiredConfigRevision uint64 `json:"required_config_revision,omitempty"`
	Maintenance            string `json:"maintenance,omitempty"`
}

type CommandAcknowledgement struct {
	Generation uint64 `json:"generation"`
	RunID      string `json:"run_id"`
	ReceivedAt string `json:"received_at"`
}

type RunStatistics map[string]any

type CommandResult struct {
	Generation      uint64         `json:"generation"`
	RunID           string         `json:"run_id"`
	JobID           string         `json:"job_id"`
	RunKind         string         `json:"run_kind"`
	Status          string         `json:"status"`
	ResultCode      string         `json:"result_code"`
	StartedAt       string         `json:"started_at"`
	FinishedAt      string         `json:"finished_at"`
	SnapshotIDs     []string       `json:"snapshot_ids"`
	Statistics      *RunStatistics `json:"statistics,omitempty"`
	Summary         string         `json:"summary,omitempty"`
	RepositoryBytes *uint64        `json:"repository_bytes,omitempty"`
}

type EventRequest struct {
	Generation uint64       `json:"generation"`
	Events     []AgentEvent `json:"events"`
}

type AgentEvent struct {
	ID             string         `json:"id"`
	RunID          string         `json:"run_id"`
	JobID          string         `json:"job_id"`
	RunKind        string         `json:"run_kind"`
	ConfigRevision uint64         `json:"config_revision"`
	Trigger        string         `json:"trigger"`
	ScheduledFor   string         `json:"scheduled_for,omitempty"`
	Sequence       uint64         `json:"sequence"`
	OccurredAt     string         `json:"occurred_at"`
	Kind           string         `json:"kind"`
	Payload        map[string]any `json:"payload"`
}

type EventsResponse struct {
	ProtocolRevision string        `json:"protocol_revision"`
	Results          []EventResult `json:"results"`
}

type EventResult struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Code   string `json:"code,omitempty"`
}

type LogCompletion struct {
	Generation   uint64 `json:"generation"`
	TotalBytes   int    `json:"total_bytes"`
	SHA256       string `json:"sha256"`
	ChunkCount   int    `json:"chunk_count"`
	Truncated    bool   `json:"truncated"`
	DroppedBytes uint64 `json:"dropped_bytes"`
}

func normalizeVersion(version string) string {
	if versionPattern.MatchString(version) {
		return version
	}
	return "0.0.0-dev"
}

func platformValues() (string, string) {
	return runtime.GOOS, runtime.GOARCH
}

func protocolTimestamp(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000000Z")
}

func validateTimestamp(value string) bool {
	if !timestampPattern.MatchString(value) {
		return false
	}
	_, err := time.Parse("2006-01-02T15:04:05.000000Z", value)
	return err == nil
}

func randomSecret() (string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate secret: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func newULID(now time.Time) (string, error) {
	value := make([]byte, 16)
	milliseconds := uint64(now.UTC().UnixMilli())
	value[0] = byte(milliseconds >> 40)
	value[1] = byte(milliseconds >> 32)
	value[2] = byte(milliseconds >> 24)
	value[3] = byte(milliseconds >> 16)
	value[4] = byte(milliseconds >> 8)
	value[5] = byte(milliseconds)
	if _, err := rand.Read(value[6:]); err != nil {
		return "", fmt.Errorf("generate enrollment attempt: %w", err)
	}

	alphabet := "0123456789abcdefghjkmnpqrstvwxyz"
	number := new(big.Int).SetBytes(value)
	base := big.NewInt(32)
	encoded := make([]byte, 26)
	for index := len(encoded) - 1; index >= 0; index-- {
		quotient, remainder := new(big.Int), new(big.Int)
		quotient.QuoRem(number, base, remainder)
		encoded[index] = alphabet[remainder.Int64()]
		number = quotient
	}
	return string(encoded), nil
}

func tokenDigest(token string) string {
	digest := sha256.Sum256([]byte(token))
	return fmt.Sprintf("%x", digest)
}

func validateBootstrap(bootstrap Bootstrap) error {
	if !validControlPlaneEndpoint(bootstrap.Endpoint) {
		return fmt.Errorf("control-plane endpoint must use HTTPS")
	}
	if !ulidPattern.MatchString(bootstrap.ServerID) || bootstrap.Generation == 0 {
		return fmt.Errorf("managed identity is invalid")
	}
	credential, err := base64.RawURLEncoding.DecodeString(bootstrap.Credential)
	if err != nil || len(credential) < 32 {
		return fmt.Errorf("managed credential is invalid")
	}
	receipt := bootstrap.SetupTokenHash
	if !digestPattern.MatchString(receipt) {
		return fmt.Errorf("managed setup receipt is invalid")
	}
	if bootstrap.Hostname == "" || runeLength(bootstrap.Hostname) > 255 {
		return fmt.Errorf("managed hostname is invalid")
	}
	return nil
}

func validControlPlaneEndpoint(value string) bool {
	endpoint, err := url.Parse(value)
	if err != nil || !endpoint.IsAbs() || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return false
	}
	return endpoint.Scheme == "https" || endpoint.Scheme == "http" && endpoint.Hostname() == "127.0.0.1"
}
