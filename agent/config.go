package agent

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/chieftools/backupchief-agent/pusher"
)

const (
	ConfigSchemaVersion uint64 = 1
	ConfigSchemaURL            = "https://pkg.backup.chief.app/config.schema.json"
)

type JobType string

const (
	JobTypeFile               JobType = "file"
	JobTypeMySQL              JobType = "mysql"
	JobTypeMySQLFiltered      JobType = "mysql_filtered"
	JobTypePostgreSQL         JobType = "postgresql"
	JobTypePostgreSQLFiltered JobType = "postgresql_filtered"
)

var (
	ErrConfigUnchanged           = errors.New("configuration is unchanged")
	ErrConfigRollback            = errors.New("configuration revision is older than the installed configuration")
	ErrConfigConflict            = errors.New("configuration revision has a different digest")
	ErrConfigCachePayloadInvalid = errors.New("installed configuration payload is invalid")
	jobConfigKeyPattern          = regexp.MustCompile(`^job_[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	storageConfigKeyPattern      = regexp.MustCompile(`^storage_[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	resourceTypePattern          = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,63}$`)
)

func isSupportedJobType(jobType JobType) bool {
	return slices.Contains([]JobType{JobTypeFile, JobTypeMySQL, JobTypeMySQLFiltered, JobTypePostgreSQL, JobTypePostgreSQLFiltered}, jobType)
}

func isMySQLJob(jobType JobType) bool {
	return slices.Contains([]JobType{JobTypeMySQL, JobTypeMySQLFiltered}, jobType)
}

func isPostgreSQLJob(jobType JobType) bool {
	return slices.Contains([]JobType{JobTypePostgreSQL, JobTypePostgreSQLFiltered}, jobType)
}

func jobTypeIntroducedIn(jobType JobType) string {
	switch jobType {
	case JobTypeFile:
		return "1.0.0"
	case JobTypeMySQL:
		return "1.1.0"
	case JobTypePostgreSQL:
		return "1.2.0"
	case JobTypeMySQLFiltered, JobTypePostgreSQLFiltered:
		return "1.6.0"
	default:
		return ""
	}
}

type Config struct {
	ProtocolRevision string
	Generation       uint64
	Revision         uint64
	SchemaVersion    uint64
	IssuedAt         string
	Host             HostConfig
	Realtime         *pusher.Config
	Destinations     map[string]Destination
	Jobs             []Job
	Warnings         []string

	unsupportedDestinations map[string]json.RawMessage
	unsupportedJobs         map[string]json.RawMessage
}

type HostConfig struct {
	Name           string
	ID             string
	Key            string
	Endpoint       string
	SetupTokenHash string
}

type Destination struct {
	Driver    string
	Path      string
	Endpoint  string
	Region    string
	Bucket    string
	Prefix    string
	AccessKey string
	SecretKey string
}

type Job struct {
	Key         string
	ID          string
	Name        string
	Type        JobType
	Enabled     bool
	Source      JobSource
	Repository  JobRepository
	Schedule    JobSchedule
	Maintenance JobMaintenance
	Retention   JobRetention
	Integrity   JobIntegrity
}

type JobMaintenance struct {
	Strategy             string
	MaxDeferralSeconds   uint64
	PruneIntervalSeconds uint64
}

type JobSource struct {
	Root          string
	OneFileSystem bool
	Excludes      []string
	MySQL         *MySQLSource
	PostgreSQL    *PostgreSQLSource
}

type MySQLSource struct {
	Host            string
	Port            uint16
	Username        string
	Password        string
	SelectionMode   string
	Databases       []string
	IncludeRoutines bool
	IncludeEvents   bool
	CustomFlags     []string
	TableSelection  *TableSelection
}

type PostgreSQLSource struct {
	Host               string
	Port               uint16
	Username           string
	Password           string
	ConnectionDatabase string
	SelectionMode      string
	Databases          []string
	TableSelection     *TableSelection
}

type TableSelection struct {
	Mode   string
	Tables []TableSelectionEntry
}

type TableSelectionEntry struct {
	Database string
	Schema   string
	Table    string
}

type JobRepository struct {
	ID              string
	Destination     string
	Path            string
	Location        string
	ServicePassword string
	Connection      RepositoryConnection
}

type RepositoryConnection struct {
	Driver    string
	Path      string
	Endpoint  string
	Region    string
	Bucket    string
	Prefix    string
	AccessKey string
	SecretKey string
}

type JobSchedule struct {
	Kind       string
	Preset     string
	Expression string
	Timezone   string
}

type JobRetention struct {
	Last                 uint64
	Hourly               uint64
	Daily                uint64
	Weekly               uint64
	Monthly              uint64
	Yearly               uint64
	KeepLatestComplete   bool
	GroupBy              string
	ForgetCron           string
	PruneCron            string
	LatestComplete       *CompleteSnapshotProof
	HasUnresolvedRuns    bool
	ProtectedSnapshotIDs []string
}

type JobIntegrity struct {
	MetadataCron string
	DataMode     string
	DataCron     string
	DataParts    uint64
}

type configDocument struct {
	Schema       string                     `json:"$schema,omitempty"`
	Metadata     configMetadataDocument     `json:"metadata"`
	Host         hostDocument               `json:"host"`
	Realtime     *pusher.Config             `json:"realtime,omitempty"`
	Destinations map[string]json.RawMessage `json:"destinations"`
	Jobs         map[string]json.RawMessage `json:"jobs"`
}

type configMetadataDocument struct {
	ProtocolRevision string `json:"protocol_revision,omitempty"`
	Generation       uint64 `json:"generation,omitempty"`
	Revision         uint64 `json:"revision,omitempty"`
	SchemaVersion    uint64 `json:"schema_version"`
	IssuedAt         string `json:"issued_at,omitempty"`
	Digest           string `json:"digest,omitempty"`
}

type hostDocument struct {
	Name string `json:"name,omitempty"`
	ID   string `json:"id,omitempty"`
}

type destinationDocument struct {
	Driver    string `json:"driver"`
	Path      string `json:"path,omitempty"`
	Endpoint  string `json:"endpoint,omitempty"`
	Region    string `json:"region,omitempty"`
	Bucket    string `json:"bucket,omitempty"`
	Prefix    string `json:"prefix,omitempty"`
	AccessKey string `json:"access_key,omitempty"`
	SecretKey string `json:"secret_key,omitempty"`
}

type jobDocument struct {
	Name        string               `json:"name,omitempty"`
	Type        JobType              `json:"type"`
	Enabled     *bool                `json:"enabled,omitempty"`
	Source      sourceDocument       `json:"source"`
	Repository  repositoryDocument   `json:"repository"`
	Schedule    string               `json:"schedule"`
	Maintenance *maintenanceDocument `json:"maintenance,omitempty"`
	Retention   *retentionDocument   `json:"retention,omitempty"`
	Integrity   *integrityDocument   `json:"integrity,omitempty"`
	Safety      *repositorySafety    `json:"safety,omitempty"`
}

type maintenanceDocument struct {
	Strategy             string `json:"strategy"`
	MaxDeferralSeconds   uint64 `json:"max_deferral_seconds"`
	PruneIntervalSeconds uint64 `json:"prune_interval_seconds"`
}

type sourceDocument struct {
	Root               string                     `json:"root,omitempty"`
	OneFileSystem      *bool                      `json:"one_file_system,omitempty"`
	Excludes           []string                   `json:"excludes,omitempty"`
	Host               string                     `json:"host,omitempty"`
	Port               uint16                     `json:"port,omitempty"`
	Username           string                     `json:"username,omitempty"`
	Password           string                     `json:"password,omitempty"`
	ConnectionDatabase string                     `json:"connection_database,omitempty"`
	Selection          *databaseSelectionDocument `json:"selection,omitempty"`
	Dump               *mysqlDumpDocument         `json:"dump,omitempty"`
	TableSelection     *tableSelectionDocument    `json:"table_selection,omitempty"`
}

type tableSelectionDocument struct {
	Mode   string                        `json:"mode"`
	Tables []tableSelectionEntryDocument `json:"tables"`
}

type tableSelectionEntryDocument struct {
	Database string `json:"database"`
	Schema   string `json:"schema,omitempty"`
	Table    string `json:"table"`
}

type databaseSelectionDocument struct {
	Mode      string   `json:"mode"`
	Databases []string `json:"databases,omitempty"`
}

type mysqlDumpDocument struct {
	IncludeRoutines bool     `json:"include_routines,omitempty"`
	IncludeEvents   bool     `json:"include_events,omitempty"`
	CustomFlags     []string `json:"custom_flags,omitempty"`
}

type repositoryDocument struct {
	ID          string `json:"id,omitempty"`
	Destination string `json:"destination"`
	Path        string `json:"path"`
	Password    string `json:"password"`
}

type retentionDocument struct {
	Last           *uint64 `json:"last,omitempty"`
	Hourly         *uint64 `json:"hourly,omitempty"`
	Daily          *uint64 `json:"daily,omitempty"`
	Weekly         *uint64 `json:"weekly,omitempty"`
	Monthly        *uint64 `json:"monthly,omitempty"`
	Yearly         *uint64 `json:"yearly,omitempty"`
	ForgetSchedule string  `json:"forget_schedule,omitempty"`
	PruneSchedule  string  `json:"prune_schedule,omitempty"`
}

type integrityDocument struct {
	MetadataSchedule string `json:"metadata_schedule,omitempty"`
	DataSchedule     string `json:"data_schedule,omitempty"`
	DataParts        uint64 `json:"data_parts,omitempty"`
}

type repositorySafety struct {
	LatestComplete       *completeSnapshotProofDocument `json:"latest_complete,omitempty"`
	HasUnresolvedRuns    bool                           `json:"has_unresolved_runs,omitempty"`
	ProtectedSnapshotIDs []string                       `json:"protected_snapshot_ids,omitempty"`
}

type completeSnapshotProofDocument struct {
	RunID       string   `json:"run_id"`
	FinishedAt  string   `json:"finished_at"`
	SnapshotIDs []string `json:"snapshot_ids"`
}

func DecodeConfig(data []byte, expectedGeneration uint64) (Config, ConfigMetadata, error) {
	if len(data) > maximumConfig {
		return Config{}, ConfigMetadata{}, fmt.Errorf("configuration exceeds 1 MiB")
	}
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return Config{}, ConfigMetadata{}, err
	}

	var document configDocument
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&document); err != nil {
		return Config{}, ConfigMetadata{}, fmt.Errorf("decode configuration: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return Config{}, ConfigMetadata{}, fmt.Errorf("decode configuration: %w", err)
	}

	fileDigest := fmt.Sprintf("%x", sha256.Sum256(data))
	metadata := ConfigMetadata{
		Generation: document.Metadata.Generation,
		Revision:   document.Metadata.Revision,
		Digest:     document.Metadata.Digest,
		FileDigest: fileDigest,
	}
	if metadata.Digest == "" {
		metadata.Digest = fileDigest
	}

	config, err := normalizeConfig(document, expectedGeneration)
	if err != nil {
		return Config{}, metadata, err
	}
	return config, metadata, nil
}

func normalizeConfig(document configDocument, expectedGeneration uint64) (Config, error) {
	managed := document.Metadata.Generation != 0 || expectedGeneration != 0
	if managed {
		if !compatibleProtocolRevision(document.Metadata.ProtocolRevision) || document.Metadata.SchemaVersion != ConfigSchemaVersion ||
			document.Metadata.Generation == 0 || document.Metadata.Revision == 0 || !validateTimestamp(document.Metadata.IssuedAt) {
			return Config{}, fmt.Errorf("managed configuration envelope is invalid")
		}
		if expectedGeneration != 0 && document.Metadata.Generation != expectedGeneration {
			return Config{}, fmt.Errorf("configuration identity does not match setup")
		}
	} else {
		if document.Metadata.SchemaVersion != ConfigSchemaVersion || document.Metadata.ProtocolRevision != "" || document.Metadata.Generation != 0 || document.Metadata.Revision != 0 || document.Metadata.IssuedAt != "" || document.Metadata.Digest != "" || document.Host.ID != "" || document.Realtime != nil {
			return Config{}, fmt.Errorf("standalone configuration cannot contain managed metadata")
		}
	}
	if document.Schema != "" && document.Schema != ConfigSchemaURL {
		return Config{}, fmt.Errorf("configuration schema URL is unsupported")
	}
	if runeLength(document.Host.Name) > 255 || strings.ContainsRune(document.Host.Name, 0) {
		return Config{}, fmt.Errorf("host name is invalid")
	}
	hostID := ""
	if document.Host.ID != "" {
		var err error
		hostID, err = parsePrefixedULID(document.Host.ID, "server_")
		if err != nil {
			return Config{}, fmt.Errorf("host id is invalid")
		}
	}
	if managed && hostID == "" {
		return Config{}, fmt.Errorf("host id is invalid")
	}
	if err := validateRealtime(document.Realtime, hostID, document.Metadata.Generation); err != nil {
		return Config{}, err
	}
	if document.Destinations == nil || len(document.Destinations) > 1000 {
		return Config{}, fmt.Errorf("configuration must define destinations")
	}
	if document.Jobs == nil || len(document.Jobs) > 1000 {
		return Config{}, fmt.Errorf("configuration jobs are invalid")
	}

	config := Config{
		ProtocolRevision: document.Metadata.ProtocolRevision,
		Generation:       document.Metadata.Generation,
		Revision:         document.Metadata.Revision,
		SchemaVersion:    document.Metadata.SchemaVersion,
		IssuedAt:         document.Metadata.IssuedAt,
		Host: HostConfig{
			Name: document.Host.Name,
			ID:   hostID,
		},
		Realtime:                document.Realtime,
		Destinations:            make(map[string]Destination, len(document.Destinations)),
		Jobs:                    make([]Job, 0, len(document.Jobs)),
		Warnings:                make([]string, 0),
		unsupportedDestinations: make(map[string]json.RawMessage),
		unsupportedJobs:         make(map[string]json.RawMessage),
	}
	destinationKeys := sortedRawMessageKeys(document.Destinations)
	for _, key := range destinationKeys {
		raw := document.Destinations[key]
		if !storageConfigKeyPattern.MatchString(key) || managed && !ulidPattern.MatchString(strings.TrimPrefix(key, "storage_")) {
			return Config{}, fmt.Errorf("destination %q has an invalid key", key)
		}
		driver, err := decodeResourceDiscriminator(raw, "driver")
		if err != nil {
			return Config{}, fmt.Errorf("destination %q: %w", key, err)
		}
		if !slices.Contains([]string{"local", "s3"}, driver) {
			config.unsupportedDestinations[key] = cloneRawMessage(raw)
			config.addWarning(fmt.Sprintf("destination %q uses unsupported driver %q; skipped", key, driver))
			continue
		}
		var document destinationDocument
		if err := json.Unmarshal(raw, &document); err != nil {
			return Config{}, fmt.Errorf("destination %q: decode: %w", key, err)
		}
		destination := Destination(document)
		if err := validateDestination(destination); err != nil {
			return Config{}, fmt.Errorf("destination %q: %w", key, err)
		}
		config.Destinations[key] = destination
	}
	jobKeys := sortedRawMessageKeys(document.Jobs)
	for _, key := range jobKeys {
		raw := document.Jobs[key]
		if !jobConfigKeyPattern.MatchString(key) || managed && !ulidPattern.MatchString(strings.TrimPrefix(key, "job_")) {
			return Config{}, fmt.Errorf("job %q: key is invalid", key)
		}
		jobType, err := decodeResourceDiscriminator(raw, "type")
		if err != nil {
			return Config{}, fmt.Errorf("job %q: %w", key, err)
		}
		if !isSupportedJobType(JobType(jobType)) {
			config.unsupportedJobs[key] = cloneRawMessage(raw)
			config.addWarning(fmt.Sprintf("job %q uses unsupported type %q; skipped", key, jobType))
			continue
		}
		if managed && jobTypeIntroducedIn(JobType(jobType)) != "" && !protocolRevisionSupports(document.Metadata.ProtocolRevision, jobTypeIntroducedIn(JobType(jobType))) {
			config.unsupportedJobs[key] = cloneRawMessage(raw)
			config.addWarning(fmt.Sprintf("job %q requires protocol revision %s; skipped", key, jobTypeIntroducedIn(JobType(jobType))))
			continue
		}
		var document jobDocument
		if err := json.Unmarshal(raw, &document); err != nil {
			return Config{}, fmt.Errorf("job %q: decode: %w", key, err)
		}
		if _, unsupported := config.unsupportedDestinations[document.Repository.Destination]; unsupported {
			config.unsupportedJobs[key] = cloneRawMessage(raw)
			config.addWarning(fmt.Sprintf("job %q references unsupported destination %q; skipped", key, document.Repository.Destination))
			continue
		}
		job, err := normalizeJob(key, document, config.Destinations, managed)
		if err != nil {
			return Config{}, fmt.Errorf("job %q: %w", key, err)
		}
		config.Jobs = append(config.Jobs, job)
	}
	slices.SortFunc(config.Jobs, func(left, right Job) int { return strings.Compare(left.Key, right.Key) })
	return config, nil
}

func validateRealtime(config *pusher.Config, hostID string, generation uint64) error {
	if config == nil {
		return nil
	}
	if config.Key == "" || len(config.Key) > 255 || config.Host == "" || len(config.Host) > 253 || config.Port < 1 || config.Port > 65535 || !config.Encrypted {
		return fmt.Errorf("realtime configuration is invalid")
	}
	hostURL, err := url.Parse("https://" + config.Host)
	if err != nil || hostURL.Hostname() != config.Host || hostURL.Port() != "" || hostURL.Path != "" {
		return fmt.Errorf("realtime host is invalid")
	}
	expectedChannel := fmt.Sprintf("private-agent.server_%s.generation_%d", hostID, generation)
	if config.Channel != expectedChannel {
		return fmt.Errorf("realtime channel does not match setup")
	}
	authURL, err := url.Parse(config.AuthURL)
	if err != nil || authURL.Scheme != "https" || authURL.Host == "" || authURL.User != nil || authURL.Fragment != "" || len(config.AuthURL) > 2048 {
		return fmt.Errorf("realtime auth URL is invalid")
	}
	return nil
}

func decodeResourceDiscriminator(data json.RawMessage, field string) (string, error) {
	var discriminator map[string]json.RawMessage
	if err := json.Unmarshal(data, &discriminator); err != nil {
		return "", fmt.Errorf("decode: %w", err)
	}
	raw, exists := discriminator[field]
	if !exists {
		return "", fmt.Errorf("%s is required", field)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || !resourceTypePattern.MatchString(value) {
		return "", fmt.Errorf("%s is invalid", field)
	}
	return value, nil
}

func sortedRawMessageKeys(values map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func cloneRawMessage(data json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), data...)
}

func normalizeTableSelection(document *tableSelectionDocument) *TableSelection {
	if document == nil {
		return nil
	}

	tables := make([]TableSelectionEntry, 0, len(document.Tables))
	for _, table := range document.Tables {
		tables = append(tables, TableSelectionEntry{Database: table.Database, Schema: table.Schema, Table: table.Table})
	}

	return &TableSelection{Mode: document.Mode, Tables: tables}
}

func encodeTableSelection(selection *TableSelection) *tableSelectionDocument {
	if selection == nil {
		return nil
	}

	tables := make([]tableSelectionEntryDocument, 0, len(selection.Tables))
	for _, table := range selection.Tables {
		tables = append(tables, tableSelectionEntryDocument{Database: table.Database, Schema: table.Schema, Table: table.Table})
	}

	return &tableSelectionDocument{Mode: selection.Mode, Tables: tables}
}

func (config *Config) addWarning(warning string) {
	const maximumWarnings = 100

	if len(config.Warnings) < maximumWarnings-1 {
		config.Warnings = append(config.Warnings, warning)
		return
	}
	if len(config.Warnings) == maximumWarnings-1 {
		config.Warnings = append(config.Warnings, "additional unsupported configuration resources were skipped")
	}
}

func normalizeJob(key string, raw jobDocument, destinations map[string]Destination, managed bool) (Job, error) {
	if !jobConfigKeyPattern.MatchString(key) {
		return Job{}, fmt.Errorf("key is invalid")
	}
	keyID := strings.TrimPrefix(key, "job_")
	if managed && !ulidPattern.MatchString(keyID) {
		return Job{}, fmt.Errorf("key is invalid")
	}
	if !isSupportedJobType(raw.Type) {
		return Job{}, fmt.Errorf("type %q is unsupported", raw.Type)
	}
	if raw.Name != "" && (runeLength(raw.Name) > 255 || strings.ContainsRune(raw.Name, 0)) {
		return Job{}, fmt.Errorf("name is invalid")
	}
	oneFileSystem := true
	if raw.Source.OneFileSystem != nil {
		oneFileSystem = *raw.Source.OneFileSystem
	}
	enabled := true
	if raw.Enabled != nil {
		enabled = *raw.Enabled
	}
	jobID := keyID
	if !ulidPattern.MatchString(jobID) {
		jobID = derivedID("job:" + key)
	}
	destination, exists := destinations[raw.Repository.Destination]
	if !exists {
		return Job{}, fmt.Errorf("repository references unknown destination %q", raw.Repository.Destination)
	}
	connection, location, err := resolveRepository(destination, raw.Repository.Path)
	if err != nil {
		return Job{}, err
	}
	repositoryID := raw.Repository.ID
	if repositoryID == "" {
		repositoryID = fmt.Sprintf("%x", sha256.Sum256([]byte(location)))
	}
	retention := defaultRetention(key)
	if raw.Retention != nil {
		applyRetention(&retention, *raw.Retention)
	}
	integrity := defaultIntegrity(key, retention)
	if raw.Integrity != nil {
		if raw.Integrity.MetadataSchedule != "" {
			integrity.MetadataCron = raw.Integrity.MetadataSchedule
		}
		if raw.Integrity.DataSchedule != "" {
			integrity.DataCron = raw.Integrity.DataSchedule
			integrity.DataMode = "custom"
		}
		if raw.Integrity.DataParts != 0 {
			integrity.DataParts = raw.Integrity.DataParts
		}
	}
	if raw.Safety != nil {
		if raw.Safety.LatestComplete != nil {
			runID, err := parsePrefixedULID(raw.Safety.LatestComplete.RunID, "run_")
			if err != nil {
				return Job{}, fmt.Errorf("latest complete run id is invalid")
			}
			retention.LatestComplete = &CompleteSnapshotProof{
				RunID:       runID,
				FinishedAt:  raw.Safety.LatestComplete.FinishedAt,
				SnapshotIDs: append([]string{}, raw.Safety.LatestComplete.SnapshotIDs...),
			}
		}
		retention.HasUnresolvedRuns = raw.Safety.HasUnresolvedRuns
		retention.ProtectedSnapshotIDs = append([]string{}, raw.Safety.ProtectedSnapshotIDs...)
	}
	jobSource := JobSource{
		Root:          raw.Source.Root,
		OneFileSystem: oneFileSystem,
		Excludes:      append([]string{}, raw.Source.Excludes...),
	}
	if isMySQLJob(raw.Type) {
		if raw.Source.Selection == nil || raw.Source.Dump == nil {
			return Job{}, fmt.Errorf("MySQL source is incomplete")
		}
		jobSource = JobSource{MySQL: &MySQLSource{
			Host:            raw.Source.Host,
			Port:            raw.Source.Port,
			Username:        raw.Source.Username,
			Password:        raw.Source.Password,
			SelectionMode:   raw.Source.Selection.Mode,
			Databases:       append([]string{}, raw.Source.Selection.Databases...),
			IncludeRoutines: raw.Source.Dump.IncludeRoutines,
			IncludeEvents:   raw.Source.Dump.IncludeEvents,
			CustomFlags:     append([]string{}, raw.Source.Dump.CustomFlags...),
			TableSelection:  normalizeTableSelection(raw.Source.TableSelection),
		}}
	} else if isPostgreSQLJob(raw.Type) {
		if raw.Source.Selection == nil || raw.Source.Dump != nil {
			return Job{}, fmt.Errorf("PostgreSQL source is incomplete")
		}
		jobSource = JobSource{PostgreSQL: &PostgreSQLSource{
			Host:               raw.Source.Host,
			Port:               raw.Source.Port,
			Username:           raw.Source.Username,
			Password:           raw.Source.Password,
			ConnectionDatabase: raw.Source.ConnectionDatabase,
			SelectionMode:      raw.Source.Selection.Mode,
			Databases:          append([]string{}, raw.Source.Selection.Databases...),
			TableSelection:     normalizeTableSelection(raw.Source.TableSelection),
		}}
	}
	job := Job{
		Key:     key,
		ID:      jobID,
		Name:    raw.Name,
		Type:    raw.Type,
		Enabled: enabled,
		Source:  jobSource,
		Repository: JobRepository{
			ID:              repositoryID,
			Destination:     raw.Repository.Destination,
			Path:            raw.Repository.Path,
			Location:        location,
			ServicePassword: raw.Repository.Password,
			Connection:      connection,
		},
		Schedule:  JobSchedule{Kind: "cron", Expression: raw.Schedule, Timezone: "UTC"},
		Retention: retention,
		Integrity: integrity,
	}
	if raw.Maintenance != nil {
		job.Maintenance = JobMaintenance{
			Strategy:             raw.Maintenance.Strategy,
			MaxDeferralSeconds:   raw.Maintenance.MaxDeferralSeconds,
			PruneIntervalSeconds: raw.Maintenance.PruneIntervalSeconds,
		}
	}
	if err := validateJob(job); err != nil {
		return Job{}, err
	}
	return job, nil
}

func validateDestination(destination Destination) error {
	switch destination.Driver {
	case "local":
		if !filepath.IsAbs(destination.Path) || strings.ContainsRune(destination.Path, 0) || runeLength(destination.Path) > 4096 ||
			destination.Endpoint != "" || destination.Region != "" || destination.Bucket != "" || destination.Prefix != "" || destination.AccessKey != "" || destination.SecretKey != "" {
			return errors.New("local settings are invalid")
		}
	case "s3":
		endpoint, err := url.Parse(destination.Endpoint)
		if err != nil || endpoint.Scheme != "https" || endpoint.Hostname() == "" || endpoint.User != nil ||
			(endpoint.Port() != "" && endpoint.Port() != "443") || (endpoint.Path != "" && endpoint.Path != "/") || endpoint.RawQuery != "" || endpoint.Fragment != "" {
			return errors.New("S3 endpoint is invalid")
		}
		if !validBucket(destination.Bucket) || destination.Region == "" || runeLength(destination.Region) > 255 ||
			(destination.Prefix != "" && !validRelativePath(destination.Prefix)) || runeLength(destination.Prefix) > 1024 ||
			destination.AccessKey == "" || runeLength(destination.AccessKey) > 1000 || destination.SecretKey == "" || runeLength(destination.SecretKey) > 1000 || destination.Path != "" {
			return errors.New("S3 settings are invalid")
		}
	default:
		return errors.New("driver is invalid")
	}
	return nil
}

func resolveRepository(destination Destination, path string) (RepositoryConnection, string, error) {
	if !validRelativePath(path) || runeLength(path) > 1024 {
		return RepositoryConnection{}, "", errors.New("repository path must be a safe relative path")
	}
	connection := RepositoryConnection{
		Driver:    destination.Driver,
		Endpoint:  destination.Endpoint,
		Region:    destination.Region,
		Bucket:    destination.Bucket,
		AccessKey: destination.AccessKey,
		SecretKey: destination.SecretKey,
	}
	if destination.Driver == "local" {
		connection.Path = filepath.Join(destination.Path, filepath.FromSlash(path))
		return connection, connection.Path, nil
	}
	connection.Prefix = path
	if destination.Prefix != "" {
		connection.Prefix = destination.Prefix + "/" + path
	}
	return connection, "s3:" + destination.Endpoint + "/" + destination.Bucket + "/" + connection.Prefix, nil
}

func validRelativePath(value string) bool {
	if value == "" || strings.ContainsAny(value, "\\\x00") || strings.HasPrefix(value, "/") {
		return false
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

func defaultRetention(key string) JobRetention {
	digest := sha256.Sum256([]byte(key))
	return JobRetention{
		Last:               12,
		Hourly:             24,
		Daily:              7,
		Weekly:             4,
		Monthly:            3,
		KeepLatestComplete: true,
		ForgetCron:         fmt.Sprintf("%d %d * * *", digest[0]%60, 1+digest[1]%4),
		PruneCron:          fmt.Sprintf("%d %d * * %d", digest[2]%60, 2+digest[3]%4, digest[4]%7),
	}
}

func applyRetention(target *JobRetention, source retentionDocument) {
	if source.Last != nil {
		target.Last = *source.Last
	}
	if source.Hourly != nil {
		target.Hourly = *source.Hourly
	}
	if source.Daily != nil {
		target.Daily = *source.Daily
	}
	if source.Weekly != nil {
		target.Weekly = *source.Weekly
	}
	if source.Monthly != nil {
		target.Monthly = *source.Monthly
	}
	if source.Yearly != nil {
		target.Yearly = *source.Yearly
	}
	if source.ForgetSchedule != "" {
		target.ForgetCron = source.ForgetSchedule
	}
	if source.PruneSchedule != "" {
		target.PruneCron = source.PruneSchedule
	}
}

func defaultIntegrity(key string, retention JobRetention) JobIntegrity {
	digest := sha256.Sum256([]byte("integrity:" + key))
	parts := uint64(4)
	if retention.Monthly > 6 || retention.Yearly > 0 {
		parts = 12
	}
	return JobIntegrity{
		MetadataCron: fmt.Sprintf("%d %d * * %d", digest[0]%60, digest[1]%5, digest[2]%7),
		DataMode:     "auto",
		DataCron:     fmt.Sprintf("%d %d * * %d", digest[3]%60, digest[4]%5, digest[5]%7),
		DataParts:    parts,
	}
}

func (store *FileStore) SaveConfig(bootstrap Bootstrap, data []byte, previous *ConfigMetadata) (ConfigMetadata, error) {
	config, metadata, err := DecodeConfig(data, bootstrap.Generation)
	if err != nil {
		return metadata, err
	}
	config.Host = HostConfig{Name: bootstrap.Hostname, ID: bootstrap.ServerID}
	if previous != nil {
		if metadata.Generation < previous.Generation || metadata.Generation == previous.Generation && metadata.Revision < previous.Revision {
			return metadata, ErrConfigRollback
		}
		if metadata.Generation == previous.Generation && metadata.Revision == previous.Revision {
			if metadata.Digest != previous.Digest && !store.configDiffersOnlyByProtocol(bootstrap, config) {
				return metadata, ErrConfigConflict
			}
			if metadata.Digest == previous.Digest {
				return metadata, ErrConfigUnchanged
			}
		}
	}
	encoded, err := encodeConfig(config, metadata.Digest)
	if err != nil {
		return metadata, err
	}
	uid, gid := store.ServiceUID, store.ServiceGID
	if err := writeAtomic(store.Paths.ManagedConfig, encoded, 0o600, uid, gid); err != nil {
		return metadata, err
	}
	metadata.FileDigest = fmt.Sprintf("%x", sha256.Sum256(encoded))
	return metadata, nil
}

func (store *FileStore) configDiffersOnlyByProtocol(bootstrap Bootstrap, incoming Config) bool {
	data, err := os.ReadFile(store.Paths.ManagedConfig)
	if err != nil {
		return false
	}
	installed, _, err := DecodeConfig(data, bootstrap.Generation)
	if err != nil {
		return false
	}

	installed.Host = HostConfig{Name: bootstrap.Hostname, ID: bootstrap.ServerID}
	installed.ProtocolRevision = ProtocolRevision
	incoming.ProtocolRevision = ProtocolRevision
	installedBody, err := encodeConfig(installed, "")
	if err != nil {
		return false
	}
	incomingBody, err := encodeConfig(incoming, "")
	if err != nil {
		return false
	}

	return bytes.Equal(installedBody, incomingBody)
}

func (store *FileStore) LoadConfig(bootstrap Bootstrap) ([]byte, ConfigMetadata, error) {
	data, err := os.ReadFile(store.Paths.ManagedConfig)
	if err != nil {
		return nil, ConfigMetadata{}, err
	}
	_, metadata, err := DecodeConfig(data, bootstrap.Generation)
	if err != nil {
		return nil, metadata, fmt.Errorf("%w: %v", ErrConfigCachePayloadInvalid, err)
	}
	return data, metadata, nil
}

func encodeConfig(config Config, managedDigest string) ([]byte, error) {
	document := configDocument{
		Schema: ConfigSchemaURL,
		Metadata: configMetadataDocument{
			ProtocolRevision: config.ProtocolRevision,
			Generation:       config.Generation,
			Revision:         config.Revision,
			SchemaVersion:    ConfigSchemaVersion,
			IssuedAt:         config.IssuedAt,
			Digest:           managedDigest,
		},
		Host: hostDocument{
			Name: config.Host.Name,
			ID:   prefixID(config.Host.ID, "server_"),
		},
		Realtime:     config.Realtime,
		Destinations: map[string]json.RawMessage{},
		Jobs:         map[string]json.RawMessage{},
	}
	if config.Generation == 0 {
		document.Metadata.ProtocolRevision = ""
		document.Metadata.Digest = ""
	}
	destinationKeys := make(map[string]string, len(config.Destinations))
	for key, raw := range config.unsupportedDestinations {
		document.Destinations[key] = cloneRawMessage(raw)
	}
	for key, destination := range config.Destinations {
		prefixedKey := prefixConfigKey(key, "storage_")
		destinationKeys[key] = prefixedKey
		raw, err := json.Marshal(destinationDocument(destination))
		if err != nil {
			return nil, fmt.Errorf("encode destination %q: %w", prefixedKey, err)
		}
		document.Destinations[prefixedKey] = raw
	}
	for key, raw := range config.unsupportedJobs {
		document.Jobs[key] = cloneRawMessage(raw)
	}
	for _, job := range config.Jobs {
		enabled := job.Enabled
		oneFileSystem := job.Source.OneFileSystem
		destinationKey := destinationKeys[job.Repository.Destination]
		if destinationKey == "" {
			destinationKey = prefixConfigKey(job.Repository.Destination, "storage_")
		}
		var latestComplete *completeSnapshotProofDocument
		if job.Retention.LatestComplete != nil {
			latestComplete = &completeSnapshotProofDocument{
				RunID:       prefixID(job.Retention.LatestComplete.RunID, "run_"),
				FinishedAt:  job.Retention.LatestComplete.FinishedAt,
				SnapshotIDs: append([]string{}, job.Retention.LatestComplete.SnapshotIDs...),
			}
		}
		jobKey := prefixConfigKey(job.Key, "job_")
		source := sourceDocument{
			Root: job.Source.Root, OneFileSystem: &oneFileSystem, Excludes: job.Source.Excludes,
		}
		if isMySQLJob(job.Type) && job.Source.MySQL != nil {
			mysql := job.Source.MySQL
			source = sourceDocument{
				Host: mysql.Host, Port: mysql.Port, Username: mysql.Username, Password: mysql.Password,
				Selection:      &databaseSelectionDocument{Mode: mysql.SelectionMode, Databases: mysql.Databases},
				Dump:           &mysqlDumpDocument{IncludeRoutines: mysql.IncludeRoutines, IncludeEvents: mysql.IncludeEvents, CustomFlags: mysql.CustomFlags},
				TableSelection: encodeTableSelection(mysql.TableSelection),
			}
		} else if isPostgreSQLJob(job.Type) && job.Source.PostgreSQL != nil {
			postgresql := job.Source.PostgreSQL
			source = sourceDocument{
				Host: postgresql.Host, Port: postgresql.Port, Username: postgresql.Username, Password: postgresql.Password,
				ConnectionDatabase: postgresql.ConnectionDatabase,
				Selection:          &databaseSelectionDocument{Mode: postgresql.SelectionMode, Databases: postgresql.Databases},
				TableSelection:     encodeTableSelection(postgresql.TableSelection),
			}
		}
		documentJob := jobDocument{
			Name:    job.Name,
			Type:    job.Type,
			Enabled: &enabled,
			Source:  source,
			Repository: repositoryDocument{
				ID:          job.Repository.ID,
				Destination: destinationKey,
				Path:        job.Repository.Path,
				Password:    job.Repository.ServicePassword,
			},
			Schedule: job.Schedule.Expression,
			Retention: &retentionDocument{
				Last:           uint64Pointer(job.Retention.Last),
				Hourly:         uint64Pointer(job.Retention.Hourly),
				Daily:          uint64Pointer(job.Retention.Daily),
				Weekly:         uint64Pointer(job.Retention.Weekly),
				Monthly:        uint64Pointer(job.Retention.Monthly),
				Yearly:         uint64Pointer(job.Retention.Yearly),
				ForgetSchedule: job.Retention.ForgetCron,
				PruneSchedule:  job.Retention.PruneCron,
			},
			Integrity: &integrityDocument{
				MetadataSchedule: job.Integrity.MetadataCron,
				DataSchedule:     job.Integrity.DataCron,
				DataParts:        job.Integrity.DataParts,
			},
			Safety: &repositorySafety{
				LatestComplete:       latestComplete,
				HasUnresolvedRuns:    job.Retention.HasUnresolvedRuns,
				ProtectedSnapshotIDs: append([]string{}, job.Retention.ProtectedSnapshotIDs...),
			},
		}
		if job.Maintenance.Strategy != "" {
			documentJob.Maintenance = &maintenanceDocument{
				Strategy:             job.Maintenance.Strategy,
				MaxDeferralSeconds:   job.Maintenance.MaxDeferralSeconds,
				PruneIntervalSeconds: job.Maintenance.PruneIntervalSeconds,
			}
		}
		raw, err := json.Marshal(documentJob)
		if err != nil {
			return nil, fmt.Errorf("encode job %q: %w", jobKey, err)
		}
		document.Jobs[jobKey] = raw
	}
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode configuration: %w", err)
	}
	return append(encoded, '\n'), nil
}

func (config Config) MarshalJSON() ([]byte, error) {
	if config.Destinations == nil {
		config.Destinations = map[string]Destination{}
	}
	for index := range config.Jobs {
		job := &config.Jobs[index]
		if job.Key == "" {
			job.Key = prefixID(job.ID, "job_")
		}
		if job.Repository.Destination != "" {
			continue
		}
		destinationKey := "storage_repository-" + job.ID
		job.Repository.Destination = destinationKey
		connection := job.Repository.Connection
		destination := Destination{
			Driver:    connection.Driver,
			Endpoint:  connection.Endpoint,
			Region:    connection.Region,
			Bucket:    connection.Bucket,
			AccessKey: connection.AccessKey,
			SecretKey: connection.SecretKey,
		}
		if connection.Driver == "local" {
			destination.Path = filepath.Dir(connection.Path)
			job.Repository.Path = filepath.Base(connection.Path)
		} else {
			job.Repository.Path = connection.Prefix
		}
		config.Destinations[destinationKey] = destination
	}
	encoded, err := encodeConfig(config, "")
	if err != nil {
		return nil, err
	}
	return bytes.TrimSpace(encoded), nil
}

func uint64Pointer(value uint64) *uint64 {
	return &value
}

const (
	maximumMySQLDatabases      = 1000
	maximumPostgreSQLDatabases = 1000
)

func validateJob(job Job) error {
	if !isSupportedJobType(job.Type) {
		return fmt.Errorf("type %q is unsupported", job.Type)
	}
	if job.ID == "" || !digestPattern.MatchString(job.ID) && !ulidPattern.MatchString(job.ID) {
		return fmt.Errorf("id is invalid")
	}
	if job.Type == JobTypeFile {
		if !filepath.IsAbs(job.Source.Root) || runeLength(job.Source.Root) > 4096 || !job.Source.OneFileSystem || len(job.Source.Excludes) > 100 || job.Source.MySQL != nil || job.Source.PostgreSQL != nil {
			return fmt.Errorf("source is invalid")
		}
		for _, exclude := range job.Source.Excludes {
			if exclude == "" || runeLength(exclude) > 4096 || strings.ContainsRune(exclude, 0) {
				return fmt.Errorf("exclude is invalid")
			}
		}
	} else if isMySQLJob(job.Type) {
		if !validMySQLJobSource(job.Type, job.Source.MySQL) {
			return fmt.Errorf("MySQL selection source is invalid")
		}
		if err := validateMySQLSource(job.Source); err != nil {
			return err
		}
	} else {
		if !validPostgreSQLJobSource(job.Type, job.Source.PostgreSQL) {
			return fmt.Errorf("PostgreSQL selection source is invalid")
		}
		if err := validatePostgreSQLSource(job.Source); err != nil {
			return err
		}
	}
	if job.Repository.Location == "" || runeLength(job.Repository.Location) > 2048 || !digestPattern.MatchString(job.Repository.ID) {
		return fmt.Errorf("repository identity is invalid")
	}
	if job.Repository.ServicePassword == "" || runeLength(job.Repository.ServicePassword) > 1024 || strings.ContainsAny(job.Repository.ServicePassword, "\r\n\x00") {
		return fmt.Errorf("repository password is invalid")
	}
	if _, err := parseFixedUTCSchedule(job.Schedule.Expression); err != nil {
		return fmt.Errorf("schedule is invalid: %w", err)
	}
	if job.Maintenance.Strategy != "" && (job.Maintenance.Strategy != "after_scheduled_backup" || job.Maintenance.MaxDeferralSeconds < 3600 || job.Maintenance.MaxDeferralSeconds > 604800 || job.Maintenance.PruneIntervalSeconds < 86400 || job.Maintenance.PruneIntervalSeconds > 2678400) {
		return fmt.Errorf("maintenance policy is invalid")
	}
	retention := job.Retention
	if retention.Last > 8760 || retention.Hourly > 8760 || retention.Daily > 3660 || retention.Weekly > 520 || retention.Monthly > 120 || retention.Yearly > 100 || !retention.KeepLatestComplete || retention.GroupBy != "" {
		return fmt.Errorf("retention is invalid")
	}
	if _, err := parseFixedUTCSchedule(retention.ForgetCron); err != nil {
		return fmt.Errorf("forget schedule is invalid: %w", err)
	}
	if _, err := parseFixedUTCSchedule(retention.PruneCron); err != nil {
		return fmt.Errorf("prune schedule is invalid: %w", err)
	}
	if proof := retention.LatestComplete; proof != nil {
		if !ulidPattern.MatchString(proof.RunID) || !validateTimestamp(proof.FinishedAt) || len(proof.SnapshotIDs) == 0 || len(proof.SnapshotIDs) > maximumMySQLDatabases {
			return fmt.Errorf("latest complete snapshot proof is invalid")
		}
		seen := map[string]bool{}
		for _, snapshotID := range proof.SnapshotIDs {
			if !digestPattern.MatchString(snapshotID) || seen[snapshotID] {
				return fmt.Errorf("latest complete snapshot proof is invalid")
			}
			seen[snapshotID] = true
		}
	}
	if len(retention.ProtectedSnapshotIDs) > 1000 {
		return fmt.Errorf("protected snapshot identities are invalid")
	}
	seenProtected := map[string]bool{}
	for index, snapshotID := range retention.ProtectedSnapshotIDs {
		if !digestPattern.MatchString(snapshotID) || seenProtected[snapshotID] || index > 0 && retention.ProtectedSnapshotIDs[index-1] >= snapshotID {
			return fmt.Errorf("protected snapshot identities are invalid")
		}
		seenProtected[snapshotID] = true
	}
	integrity := job.Integrity
	if !slices.Contains([]string{"auto", "custom"}, integrity.DataMode) || integrity.DataParts < 2 || integrity.DataParts > 12 {
		return fmt.Errorf("integrity settings are invalid")
	}
	if _, err := parseFixedUTCSchedule(integrity.MetadataCron); err != nil {
		return fmt.Errorf("metadata check schedule is invalid: %w", err)
	}
	if _, err := parseFixedUTCSchedule(integrity.DataCron); err != nil {
		return fmt.Errorf("data check schedule is invalid: %w", err)
	}
	return nil
}

func validMySQLJobSource(jobType JobType, source *MySQLSource) bool {
	if source == nil {
		return false
	}

	switch jobType {
	case JobTypeMySQL:
		return source.TableSelection == nil && source.SelectionMode != "exclude"
	case JobTypeMySQLFiltered:
		return source.TableSelection != nil || source.SelectionMode == "exclude"
	default:
		return false
	}
}

func validPostgreSQLJobSource(jobType JobType, source *PostgreSQLSource) bool {
	if source == nil {
		return false
	}

	switch jobType {
	case JobTypePostgreSQL:
		return source.TableSelection == nil && source.SelectionMode != "exclude"
	case JobTypePostgreSQLFiltered:
		return source.TableSelection != nil || source.SelectionMode == "exclude"
	default:
		return false
	}
}

func validateMySQLSource(source JobSource) error {
	mysql := source.MySQL
	if mysql == nil || source.PostgreSQL != nil || source.Root != "" || len(source.Excludes) != 0 || mysql.Host == "" || runeLength(mysql.Host) > 255 || strings.ContainsAny(mysql.Host, "\r\n\x00") || mysql.Port == 0 || mysql.Username == "" || runeLength(mysql.Username) > 255 || strings.ContainsAny(mysql.Username, "\r\n\x00") || runeLength(mysql.Password) > 4096 || strings.ContainsRune(mysql.Password, 0) {
		return fmt.Errorf("MySQL source is invalid")
	}
	if !slices.Contains([]string{"selected", "all_accessible", "exclude"}, mysql.SelectionMode) || len(mysql.Databases) > maximumMySQLDatabases || mysql.SelectionMode != "all_accessible" && len(mysql.Databases) == 0 || mysql.SelectionMode == "all_accessible" && len(mysql.Databases) != 0 {
		return fmt.Errorf("MySQL database selection is invalid")
	}
	seen := map[string]bool{}
	for _, database := range mysql.Databases {
		if database == "" || runeLength(database) > 64 || strings.ContainsAny(database, "\r\n\x00") || seen[database] {
			return fmt.Errorf("MySQL database selection is invalid")
		}
		seen[database] = true
	}
	if len(mysql.CustomFlags) > 50 {
		return fmt.Errorf("MySQL dump options are invalid")
	}
	for _, flag := range mysql.CustomFlags {
		if !validMySQLFlag(flag) {
			return fmt.Errorf("MySQL dump options are invalid")
		}
	}
	if err := validateTableSelection(mysql.TableSelection, mysql.SelectionMode, mysql.Databases, false); err != nil {
		return fmt.Errorf("MySQL table selection is invalid")
	}
	return nil
}

func validatePostgreSQLSource(source JobSource) error {
	postgresql := source.PostgreSQL
	if postgresql == nil || source.MySQL != nil || source.Root != "" || len(source.Excludes) != 0 || postgresql.Host == "" || runeLength(postgresql.Host) > 255 || strings.ContainsAny(postgresql.Host, "\r\n\x00") || postgresql.Port == 0 || postgresql.Username == "" || runeLength(postgresql.Username) > 255 || strings.ContainsAny(postgresql.Username, "\r\n\x00") || runeLength(postgresql.Password) > 4096 || strings.ContainsAny(postgresql.Password, "\r\n\x00") || postgresql.ConnectionDatabase == "" || runeLength(postgresql.ConnectionDatabase) > 63 || strings.ContainsAny(postgresql.ConnectionDatabase, "\r\n\x00") {
		return fmt.Errorf("PostgreSQL source is invalid")
	}
	if !slices.Contains([]string{"selected", "all_accessible", "exclude"}, postgresql.SelectionMode) || len(postgresql.Databases) > maximumPostgreSQLDatabases || postgresql.SelectionMode != "all_accessible" && len(postgresql.Databases) == 0 || postgresql.SelectionMode == "all_accessible" && len(postgresql.Databases) != 0 {
		return fmt.Errorf("PostgreSQL database selection is invalid")
	}
	seen := map[string]bool{}
	for _, database := range postgresql.Databases {
		lower := strings.ToLower(database)
		if database == "" || runeLength(database) > 63 || strings.ContainsAny(database, "\r\n\x00") || seen[database] || lower == "template0" || lower == "template1" {
			return fmt.Errorf("PostgreSQL database selection is invalid")
		}
		seen[database] = true
	}
	if err := validateTableSelection(postgresql.TableSelection, postgresql.SelectionMode, postgresql.Databases, true); err != nil {
		return fmt.Errorf("PostgreSQL table selection is invalid")
	}
	return nil
}

func validateTableSelection(selection *TableSelection, selectionMode string, databases []string, postgresql bool) error {
	if selection == nil {
		return nil
	}
	if !slices.Contains([]string{"include", "exclude"}, selection.Mode) || len(selection.Tables) == 0 || len(selection.Tables) > 1000 || selection.Mode == "include" && selectionMode != "selected" {
		return fmt.Errorf("table selection is invalid")
	}

	selected := make(map[string]bool, len(databases))
	for _, database := range databases {
		selected[database] = true
	}
	covered := make(map[string]bool, len(databases))
	seen := make(map[string]bool, len(selection.Tables))
	for _, table := range selection.Tables {
		maximum := 64
		if postgresql {
			maximum = 63
		}
		if table.Database == "" || table.Table == "" || runeLength(table.Database) > maximum || runeLength(table.Table) > maximum || containsControl(table.Database) || containsControl(table.Table) {
			return fmt.Errorf("table selection is invalid")
		}
		if postgresql {
			if table.Schema == "" || runeLength(table.Schema) > maximum || containsControl(table.Schema) || strings.EqualFold(table.Database, "template0") || strings.EqualFold(table.Database, "template1") {
				return fmt.Errorf("table selection is invalid")
			}
		} else if table.Schema != "" || mysqlSystemDatabases[strings.ToLower(table.Database)] || strings.HasPrefix(table.Database, "-") || strings.HasPrefix(table.Table, "-") || selection.Mode == "exclude" && (strings.Contains(table.Database, ".") || strings.Contains(table.Table, ".")) {
			return fmt.Errorf("table selection is invalid")
		}
		if selectionMode == "selected" && !selected[table.Database] {
			return fmt.Errorf("table selection is invalid")
		}
		if selectionMode == "exclude" && selected[table.Database] {
			return fmt.Errorf("table selection is invalid")
		}
		key := table.Database + "\x00" + table.Schema + "\x00" + table.Table
		if seen[key] {
			return fmt.Errorf("table selection is invalid")
		}
		seen[key] = true
		covered[table.Database] = true
	}
	if selection.Mode == "include" {
		for _, database := range databases {
			if !covered[database] {
				return fmt.Errorf("table selection is invalid")
			}
		}
	}

	return nil
}

func containsControl(value string) bool {
	return strings.IndexFunc(value, unicode.IsControl) >= 0
}

var mysqlFlagPattern = regexp.MustCompile(`^--[a-z0-9][a-z0-9-]*(?:=[^\x00\r\n]{0,480})?$`)

func validMySQLFlag(flag string) bool {
	if len(flag) > 512 || !mysqlFlagPattern.MatchString(flag) {
		return false
	}
	name := strings.TrimPrefix(strings.SplitN(flag, "=", 2)[0], "--")
	forbidden := map[string]bool{
		"all-databases": true, "all-tablespaces": true, "column-statistics": true, "databases": true,
		"debug": true, "debug-info": true, "defaults-extra-file": true, "defaults-file": true,
		"delete-master-logs": true, "delete-source-logs": true, "events": true, "flush-logs": true,
		"force": true, "help": true, "host": true, "ignore-database": true, "ignore-error": true,
		"ignore-table": true, "init-command": true, "init-command-add": true, "lock-all-tables": true,
		"lock-tables": true, "log-error": true, "login-path": true, "master-data": true,
		"no-create-info": true, "no-data": true, "no-tablespaces": true, "password": true,
		"password1": true, "password2": true, "password3": true, "pipe": true, "port": true,
		"print-defaults": true, "protocol": true, "quick": true, "result-file": true, "routines": true,
		"single-transaction": true, "skip-events": true, "skip-lock-tables": true, "skip-quick": true,
		"skip-routines": true, "skip-single-transaction": true, "skip-triggers": true, "socket": true,
		"source-data": true, "tab": true, "tables": true, "user": true, "version": true,
		"where": true, "xml": true,
	}
	return !forbidden[name] && !strings.HasPrefix(name, "fields-") && !strings.HasPrefix(name, "lines-")
}

func derivedID(value string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(value)))
}

func parsePrefixedULID(value, prefix string) (string, error) {
	if !strings.HasPrefix(value, prefix) {
		return "", fmt.Errorf("missing %s prefix", strings.TrimSuffix(prefix, "_"))
	}
	id := strings.TrimPrefix(value, prefix)
	if !ulidPattern.MatchString(id) {
		return "", fmt.Errorf("invalid id")
	}
	return id, nil
}

func prefixID(value, prefix string) string {
	if value == "" || strings.HasPrefix(value, prefix) {
		return value
	}
	return prefix + value
}

func prefixConfigKey(value, prefix string) string {
	if strings.HasPrefix(value, prefix) {
		return value
	}
	return prefix + value
}

func validBucket(bucket string) bool {
	if len(bucket) < 3 || len(bucket) > 63 || !validBucketEdge(bucket[0]) {
		return false
	}
	for _, character := range bucket {
		if character != '.' && character != '-' && (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return false
		}
	}
	return validBucketEdge(bucket[len(bucket)-1])
}

func validBucketEdge(character byte) bool {
	return character >= 'a' && character <= 'z' || character >= '0' && character <= '9'
}

func runeLength(value string) int {
	return utf8.RuneCountInString(value)
}

func rejectDuplicateJSONKeys(data []byte) error {
	if !utf8.Valid(data) {
		return fmt.Errorf("validate JSON: input is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := walkJSONValue(decoder); err != nil {
		return fmt.Errorf("validate JSON: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return fmt.Errorf("validate JSON: %w", err)
	}
	return nil
}

func walkJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		keys := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("object key is not a string")
			}
			if _, exists := keys[key]; exists {
				return fmt.Errorf("duplicate object key %q", key)
			}
			keys[key] = struct{}{}
			if err := walkJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := walkJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}
