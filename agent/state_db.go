package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	bolt "go.etcd.io/bbolt"
)

const (
	stateFormatVersion = 1
	SpoolReserveBytes  = 16 << 20
)

var (
	metadataBucket    = []byte("metadata")
	commandsBucket    = []byte("commands")
	runsBucket        = []byte("runs")
	eventsBucket      = []byte("events")
	occurrencesBucket = []byte("occurrences")
	logRefsBucket     = []byte("log_refs")
	formatVersionKey  = []byte("format_version")
	legacyImportedKey = []byte("legacy_imported")
	runtimeStateKey   = []byte("runtime_state")
	ErrSpoolCapacity  = errors.New("local spool capacity is exhausted")
)

func (store *FileStore) loadRuntimeState() (RuntimeState, error) {
	store.stateMu.Lock()
	defer store.stateMu.Unlock()

	var state RuntimeState
	err := store.viewState(func(transaction *bolt.Tx) error {
		data := transaction.Bucket(metadataBucket).Get(runtimeStateKey)
		if data == nil {
			return nil
		}
		return json.Unmarshal(data, &state)
	})
	return state, err
}

func (store *FileStore) saveRuntimeState(state RuntimeState) error {
	store.stateMu.Lock()
	defer store.stateMu.Unlock()

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode runtime state: %w", err)
	}
	return store.updateState(func(transaction *bolt.Tx) error {
		return transaction.Bucket(metadataBucket).Put(runtimeStateKey, data)
	})
}

func (store *FileStore) loadCommandJournal() (CommandJournal, error) {
	store.stateMu.Lock()
	defer store.stateMu.Unlock()

	journal := newCommandJournal()
	err := store.viewState(func(transaction *bolt.Tx) error {
		runs := transaction.Bucket(runsBucket)
		events := transaction.Bucket(eventsBucket)
		commands := transaction.Bucket(commandsBucket)

		return runs.ForEach(func(_, value []byte) error {
			var command JournalCommand
			if err := json.Unmarshal(value, &command); err != nil {
				return fmt.Errorf("decode durable run: %w", err)
			}
			if !ulidPattern.MatchString(command.Command.ID) || !ulidPattern.MatchString(command.RunID) {
				return fmt.Errorf("durable run identity is invalid")
			}
			if commands.Get([]byte(command.Command.ID)) == nil {
				return fmt.Errorf("durable command reference is missing")
			}

			prefix := []byte(command.Command.ID + "\x00")
			cursor := events.Cursor()
			for key, eventValue := cursor.Seek(prefix); key != nil && strings.HasPrefix(string(key), string(prefix)); key, eventValue = cursor.Next() {
				var event AgentEvent
				if err := json.Unmarshal(eventValue, &event); err != nil {
					return fmt.Errorf("decode durable event: %w", err)
				}
				command.Events = append(command.Events, event)
			}
			normalizeJournalCommand(&command)
			journal.Commands[command.Command.ID] = &command
			return nil
		})
	})
	return journal, err
}

func (store *FileStore) saveCommandJournal(journal CommandJournal) error {
	store.stateMu.Lock()
	defer store.stateMu.Unlock()

	return store.updateState(func(transaction *bolt.Tx) error {
		for _, name := range [][]byte{commandsBucket, runsBucket, eventsBucket, logRefsBucket} {
			bucket := transaction.Bucket(name)
			if err := clearBucket(bucket); err != nil {
				return err
			}
		}

		ids := make([]string, 0, len(journal.Commands))
		for id := range journal.Commands {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			command := journal.Commands[id]
			if command == nil || id != command.Command.ID || !ulidPattern.MatchString(id) || !ulidPattern.MatchString(command.RunID) {
				return fmt.Errorf("command journal identity is invalid")
			}
			commandData, err := json.Marshal(command.Command)
			if err != nil {
				return fmt.Errorf("encode durable command: %w", err)
			}
			if err := transaction.Bucket(commandsBucket).Put([]byte(id), commandData); err != nil {
				return err
			}

			run := *command
			run.Events = nil
			runData, err := json.Marshal(run)
			if err != nil {
				return fmt.Errorf("encode durable run: %w", err)
			}
			if err := transaction.Bucket(runsBucket).Put([]byte(command.Command.ID), runData); err != nil {
				return err
			}
			for _, event := range command.Events {
				eventData, marshalErr := json.Marshal(event)
				if marshalErr != nil {
					return fmt.Errorf("encode durable event: %w", marshalErr)
				}
				key := fmt.Sprintf("%s\x00%020d\x00%s", command.Command.ID, event.Sequence, event.ID)
				if err := transaction.Bucket(eventsBucket).Put([]byte(key), eventData); err != nil {
					return err
				}
			}
			if command.LogID != "" {
				if err := transaction.Bucket(logRefsBucket).Put([]byte(command.LogID), []byte(command.RunID)); err != nil {
					return err
				}
			}
			if command.Trigger == "scheduled" && command.ScheduledFor != "" {
				key := occurrenceKey(command.Command.Payload.JobID, command.RunKind, command.ScheduledFor)
				if existing := transaction.Bucket(occurrencesBucket).Get(key); existing != nil && string(existing) != command.RunID {
					return fmt.Errorf("scheduled occurrence identity conflicts")
				}
				if err := transaction.Bucket(occurrencesBucket).Put(key, []byte(command.RunID)); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (store *FileStore) occurrenceExists(jobID, runKind, scheduledFor string) (bool, error) {
	store.stateMu.Lock()
	defer store.stateMu.Unlock()

	found := false
	err := store.viewState(func(transaction *bolt.Tx) error {
		found = transaction.Bucket(occurrencesBucket).Get(occurrenceKey(jobID, runKind, scheduledFor)) != nil
		if !found && runKind == "backup" {
			found = transaction.Bucket(occurrencesBucket).Get(legacyOccurrenceKey(jobID, scheduledFor)) != nil
		}
		return nil
	})
	return found, err
}

func (store *FileStore) recordSkippedOccurrence(jobID, runKind, scheduledFor string, state *RuntimeState) error {
	store.stateMu.Lock()
	defer store.stateMu.Unlock()

	return store.updateState(func(transaction *bolt.Tx) error {
		if err := transaction.Bucket(occurrencesBucket).Put(occurrenceKey(jobID, runKind, scheduledFor), []byte("gap")); err != nil {
			return err
		}
		if state != nil {
			data, err := json.Marshal(state)
			if err != nil {
				return err
			}
			return transaction.Bucket(metadataBucket).Put(runtimeStateKey, data)
		}
		return nil
	})
}

func occurrenceKey(jobID, runKind, scheduledFor string) []byte {
	return []byte(jobID + "\x00" + runKind + "\x00" + scheduledFor)
}

func legacyOccurrenceKey(jobID, scheduledFor string) []byte {
	return []byte(jobID + "\x00" + scheduledFor)
}

func clearBucket(bucket *bolt.Bucket) error {
	cursor := bucket.Cursor()
	for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
		if err := cursor.Delete(); err != nil {
			return err
		}
	}
	return nil
}

func (store *FileStore) viewState(action func(*bolt.Tx) error) error {
	database, err := store.openStateDatabase()
	if err != nil {
		return err
	}
	defer database.Close()
	return database.View(action)
}

func (store *FileStore) updateState(action func(*bolt.Tx) error) error {
	database, err := store.openStateDatabase()
	if err != nil {
		return err
	}
	defer database.Close()
	err = database.Update(action)
	if isDiskFull(err) {
		_ = store.releaseReserve()
		return fmt.Errorf("%w: %v", ErrSpoolCapacity, err)
	}
	return err
}

func (store *FileStore) openStateDatabase() (*bolt.DB, error) {
	if err := os.MkdirAll(filepath.Dir(store.databasePath()), 0o700); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	database, err := bolt.Open(store.databasePath(), 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("open state database: %w", err)
	}
	if err := store.initializeStateDatabase(database); err != nil {
		_ = database.Close()
		return nil, err
	}
	if err := os.Chmod(store.databasePath(), 0o600); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("set state database permissions: %w", err)
	}
	if store.ServiceUID >= 0 && store.ServiceGID >= 0 {
		if err := os.Chown(store.databasePath(), store.ServiceUID, store.ServiceGID); err != nil {
			_ = database.Close()
			return nil, fmt.Errorf("set state database ownership: %w", err)
		}
	}
	if err := store.ensureReserve(); err != nil {
		_ = database.Close()
		return nil, err
	}
	return database, nil
}

func (store *FileStore) initializeStateDatabase(database *bolt.DB) error {
	needsImport := false
	err := database.Update(func(transaction *bolt.Tx) error {
		for _, name := range [][]byte{metadataBucket, commandsBucket, runsBucket, eventsBucket, occurrencesBucket, logRefsBucket} {
			if _, err := transaction.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		metadata := transaction.Bucket(metadataBucket)
		version := metadata.Get(formatVersionKey)
		if version != nil {
			parsed, err := strconv.Atoi(string(version))
			if err != nil || parsed != stateFormatVersion {
				return fmt.Errorf("unsupported state database format")
			}
		} else if err := metadata.Put(formatVersionKey, []byte(strconv.Itoa(stateFormatVersion))); err != nil {
			return err
		}
		needsImport = metadata.Get(legacyImportedKey) == nil
		return nil
	})
	if err != nil || !needsImport {
		return err
	}
	return store.importLegacyState(database)
}

func (store *FileStore) importLegacyState(database *bolt.DB) error {
	var state RuntimeState
	stateErr := readJSON(store.Paths.State, &state)
	if stateErr != nil && !errors.Is(stateErr, os.ErrNotExist) {
		return fmt.Errorf("read legacy runtime state: %w", stateErr)
	}

	stateFound := stateErr == nil

	var journal CommandJournal
	journalErr := readJSON(store.commandsPath(), &journal)
	if journalErr != nil && !errors.Is(journalErr, os.ErrNotExist) {
		return fmt.Errorf("read legacy command state: %w", journalErr)
	}

	journalFound := journalErr == nil

	err := database.Update(func(transaction *bolt.Tx) error {
		if stateFound {
			data, marshalErr := json.Marshal(state)
			if marshalErr != nil {
				return marshalErr
			}
			if err := transaction.Bucket(metadataBucket).Put(runtimeStateKey, data); err != nil {
				return err
			}
		}

		if journalFound {
			for id, command := range journal.Commands {
				if command == nil || id != command.Command.ID {
					return fmt.Errorf("legacy command journal identity is invalid")
				}
				normalizeJournalCommand(command)
				commandData, marshalErr := json.Marshal(command.Command)
				if marshalErr != nil {
					return marshalErr
				}
				if err := transaction.Bucket(commandsBucket).Put([]byte(id), commandData); err != nil {
					return err
				}

				run := *command
				run.Events = nil
				runData, marshalErr := json.Marshal(run)
				if marshalErr != nil {
					return marshalErr
				}
				if err := transaction.Bucket(runsBucket).Put([]byte(command.Command.ID), runData); err != nil {
					return err
				}

				for _, event := range command.Events {
					eventData, marshalErr := json.Marshal(event)
					if marshalErr != nil {
						return marshalErr
					}
					key := fmt.Sprintf("%s\x00%020d\x00%s", command.Command.ID, event.Sequence, event.ID)
					if err := transaction.Bucket(eventsBucket).Put([]byte(key), eventData); err != nil {
						return err
					}
				}
			}
		}

		return transaction.Bucket(metadataBucket).Put(legacyImportedKey, []byte("1"))
	})
	if err != nil {
		return fmt.Errorf("import legacy state: %w", err)
	}

	for _, path := range []string{store.Paths.State, store.commandsPath()} {
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return fmt.Errorf("remove imported legacy state: %w", removeErr)
		}
	}

	return nil
}

func (store *FileStore) spoolUsage() uint64 {
	var total uint64
	for _, path := range []string{store.databasePath(), store.reservePath()} {
		if info, err := os.Stat(path); err == nil {
			total += uint64(info.Size())
		}
	}
	entries, err := os.ReadDir(store.logsPath())
	if err != nil {
		return total
	}
	for _, entry := range entries {
		if info, infoErr := entry.Info(); infoErr == nil {
			total += uint64(info.Size())
		}
	}
	return total
}

func (store *FileStore) canAcceptWork(estimate uint64) bool {
	store.reserveMu.Lock()
	released := store.reserveReleased
	store.reserveMu.Unlock()
	if released {
		return false
	}
	limit := store.spoolLimit
	if limit == 0 {
		limit = SpoolBytesLimit
	}
	return store.spoolUsage()+estimate <= limit
}

func (store *FileStore) reservePath() string {
	return filepath.Join(filepath.Dir(store.databasePath()), "spool.reserve")
}

func (store *FileStore) ensureReserve() error {
	store.reserveMu.Lock()
	defer store.reserveMu.Unlock()
	if store.reserveReleased {
		return nil
	}
	if store.reserveBytes == 0 {
		return nil
	}
	if info, err := os.Stat(store.reservePath()); err == nil && uint64(info.Size()) == store.reserveBytes {
		return nil
	}
	file, err := os.OpenFile(store.reservePath(), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create spool reserve: %w", err)
	}
	block := make([]byte, 1<<20)
	remaining := store.reserveBytes
	for remaining > 0 {
		length := min(uint64(len(block)), remaining)
		if _, err := file.Write(block[:length]); err != nil {
			_ = file.Close()
			return fmt.Errorf("allocate spool reserve: %w", err)
		}
		remaining -= length
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync spool reserve: %w", err)
	}
	return file.Close()
}

func (store *FileStore) releaseReserve() error {
	store.reserveMu.Lock()
	defer store.reserveMu.Unlock()
	store.reserveReleased = true
	err := os.Remove(store.reservePath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (store *FileStore) compactStateDatabase() error {
	store.stateMu.Lock()
	defer store.stateMu.Unlock()

	source, err := store.openStateDatabase()
	if err != nil {
		return err
	}

	temporary, err := os.CreateTemp(filepath.Dir(store.databasePath()), ".state-compact-*")
	if err != nil {
		_ = source.Close()
		return fmt.Errorf("create compacted state database: %w", err)
	}
	temporaryPath := temporary.Name()
	if err := temporary.Close(); err != nil {
		_ = source.Close()
		_ = os.Remove(temporaryPath)
		return err
	}
	if err := os.Remove(temporaryPath); err != nil {
		_ = source.Close()
		return err
	}

	target, err := bolt.Open(temporaryPath, 0o600, nil)
	if err != nil {
		_ = source.Close()
		return fmt.Errorf("open compacted state database: %w", err)
	}
	compactErr := bolt.Compact(target, source, 0)
	closeTargetErr := target.Close()
	closeSourceErr := source.Close()
	if compactErr != nil || closeTargetErr != nil || closeSourceErr != nil {
		_ = os.Remove(temporaryPath)
		return errors.Join(compactErr, closeTargetErr, closeSourceErr)
	}

	if err := os.Rename(temporaryPath, store.databasePath()); err != nil {
		_ = os.Remove(temporaryPath)
		return fmt.Errorf("replace compacted state database: %w", err)
	}

	directory, err := os.Open(filepath.Dir(store.databasePath()))
	if err != nil {
		return fmt.Errorf("open compacted state directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync compacted state directory: %w", err)
	}

	return nil
}

func (store *FileStore) compactIfPressured() error {
	limit := store.spoolLimit
	if limit == 0 {
		limit = SpoolBytesLimit
	}
	if store.spoolUsage() < limit*3/4 {
		return nil
	}
	return store.compactStateDatabase()
}

func isDiskFull(err error) bool {
	return errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT)
}
