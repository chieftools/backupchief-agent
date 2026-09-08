package agent

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const commandJournalVersion = 1

type CommandJournal struct {
	Version  int                        `json:"version"`
	Commands map[string]*JournalCommand `json:"commands"`
}

type JournalCommand struct {
	Command         AgentCommand     `json:"command"`
	RunID           string           `json:"run_id"`
	ConfigRevision  uint64           `json:"config_revision"`
	RunKind         string           `json:"run_kind"`
	Trigger         string           `json:"trigger"`
	ScheduledFor    string           `json:"scheduled_for,omitempty"`
	JobSnapshot     string           `json:"job_snapshot,omitempty"`
	ReceivedAt      string           `json:"received_at"`
	Acknowledged    bool             `json:"acknowledged"`
	State           string           `json:"state"`
	Sequence        uint64           `json:"sequence"`
	Events          []AgentEvent     `json:"events"`
	Result          *CommandResult   `json:"result,omitempty"`
	ResultReported  bool             `json:"result_reported"`
	LogID           string           `json:"log_id,omitempty"`
	LogBytes        int              `json:"log_bytes,omitempty"`
	LogSHA256       string           `json:"log_sha256,omitempty"`
	LogTruncated    bool             `json:"log_truncated,omitempty"`
	LogDroppedBytes uint64           `json:"log_dropped_bytes,omitempty"`
	NextLogChunk    int              `json:"next_log_chunk,omitempty"`
	LogCompleted    bool             `json:"log_completed,omitempty"`
	MaintenancePlan *MaintenancePlan `json:"maintenance_plan,omitempty"`
}

func (store *FileStore) LoadCommandJournal() (CommandJournal, error) {
	journal, err := store.loadCommandJournal()
	if err != nil {
		return CommandJournal{}, err
	}
	if journal.Version != commandJournalVersion || journal.Commands == nil {
		return CommandJournal{}, fmt.Errorf("command journal is invalid")
	}
	for id, command := range journal.Commands {
		if command == nil || id != command.Command.ID || !ulidPattern.MatchString(id) || !ulidPattern.MatchString(command.RunID) {
			return CommandJournal{}, fmt.Errorf("command journal identity is invalid")
		}
	}
	return journal, nil
}

func normalizeJournalCommand(command *JournalCommand) {
	if command.Trigger == "" {
		command.Trigger = "manual"
	}
	if command.ConfigRevision == 0 {
		command.ConfigRevision = command.Command.Payload.RequiredConfigRevision
	}
	if command.RunKind == "" {
		command.RunKind = command.Command.Payload.Maintenance
		if command.RunKind == "" {
			command.RunKind = "backup"
		}
	}
	for index := range command.Events {
		event := &command.Events[index]
		if event.RunKind == "" {
			event.RunKind = command.RunKind
		}
		if event.ConfigRevision == 0 {
			event.ConfigRevision = command.ConfigRevision
		}
		if event.Trigger == "" {
			event.Trigger = command.Trigger
		}
		if event.ScheduledFor == "" {
			event.ScheduledFor = command.ScheduledFor
		}
	}
	if command.Result != nil && command.Result.RunKind == "" {
		command.Result.RunKind = command.RunKind
	}
}

func (store *FileStore) SaveCommandJournal(journal CommandJournal) error {
	if journal.Version != commandJournalVersion || journal.Commands == nil {
		return fmt.Errorf("command journal is invalid")
	}
	return store.saveCommandJournal(journal)
}

func (store *FileStore) WriteRunLog(logID string, contents []byte) error {
	if !ulidPattern.MatchString(logID) || len(contents) > maximumRunLog {
		return fmt.Errorf("run log is invalid")
	}
	if !store.canAcceptWork(uint64(len(contents))) {
		_ = store.releaseReserve()
		return ErrSpoolCapacity
	}
	err := writeAtomic(store.runLogPath(logID), contents, 0o600, store.ServiceUID, store.ServiceGID)
	if isDiskFull(err) {
		_ = store.releaseReserve()
		return fmt.Errorf("%w: %v", ErrSpoolCapacity, err)
	}
	return err
}

func (store *FileStore) ReadRunLog(logID string) ([]byte, error) {
	if !ulidPattern.MatchString(logID) {
		return nil, fmt.Errorf("run log identity is invalid")
	}
	data, err := os.ReadFile(store.runLogPath(logID))
	if err != nil {
		return nil, err
	}
	if len(data) > maximumRunLog {
		return nil, fmt.Errorf("run log exceeds its limit")
	}
	return data, nil
}

func (store *FileStore) ReadChecksummedRunLog(logID, expectedDigest string) ([]byte, error) {
	data, err := store.ReadRunLog(logID)
	if err != nil {
		return nil, err
	}
	actual := fmt.Sprintf("%x", sha256.Sum256(data))
	if !digestPattern.MatchString(expectedDigest) || actual != expectedDigest {
		return nil, fmt.Errorf("run log checksum does not match its durable reference")
	}
	return data, nil
}

func (store *FileStore) RemoveRunLog(logID string) error {
	if !ulidPattern.MatchString(logID) {
		return fmt.Errorf("run log identity is invalid")
	}
	err := os.Remove(store.runLogPath(logID))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (store *FileStore) runLogPath(logID string) string {
	return filepath.Join(store.logsPath(), logID+".log")
}

func newCommandJournal() CommandJournal {
	return CommandJournal{Version: commandJournalVersion, Commands: map[string]*JournalCommand{}}
}

func sameCommand(first, second AgentCommand) bool {
	left, leftErr := json.Marshal(first)
	right, rightErr := json.Marshal(second)
	return leftErr == nil && rightErr == nil && bytes.Equal(left, right)
}
