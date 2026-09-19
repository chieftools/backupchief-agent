package agent

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
)

var ErrManagedIdentityMissing = errors.New("managed identity is not configured")

type Paths struct {
	Identity         string
	Pending          string
	StandaloneConfig string
	ManagedConfig    string
	State            string
	Commands         string
	Database         string
	Logs             string
}

func DefaultPaths() Paths {
	return Paths{
		Identity:         "/etc/backupchief/identity.json",
		Pending:          "/etc/backupchief/setup.json",
		StandaloneConfig: "/etc/backupchief/config.json",
		ManagedConfig:    "/var/lib/backupchief/config.json",
		State:            "/var/lib/backupchief/runtime.json",
		Commands:         "/var/lib/backupchief/commands.json",
		Database:         "/var/lib/backupchief/state.db",
		Logs:             "/var/lib/backupchief/run-logs",
	}
}

type identityDocument struct {
	ProtocolRevision string `json:"protocol_revision"`
	Endpoint         string `json:"endpoint"`
	ServerID         string `json:"server_id"`
	Generation       uint64 `json:"generation"`
	Credential       string `json:"credential"`
	Hostname         string `json:"hostname"`
	SetupTokenHash   string `json:"setup_token_hash"`
}

func (store *FileStore) commandsPath() string {
	if store.Paths.Commands != "" {
		return store.Paths.Commands
	}
	return filepath.Join(filepath.Dir(store.Paths.State), "commands.json")
}

func (store *FileStore) logsPath() string {
	if store.Paths.Logs != "" {
		return store.Paths.Logs
	}
	return filepath.Join(filepath.Dir(store.Paths.State), "run-logs")
}

func (store *FileStore) databasePath() string {
	if store.Paths.Database != "" {
		return store.Paths.Database
	}
	return filepath.Join(filepath.Dir(store.Paths.State), "state.db")
}

func (store *FileStore) stateDirectory() string {
	return filepath.Dir(store.Paths.State)
}

type FileStore struct {
	Paths           Paths
	ServiceUID      int
	ServiceGID      int
	bootstrapMode   os.FileMode
	stateMu         sync.Mutex
	reserveMu       sync.Mutex
	spoolLimit      uint64
	reserveBytes    uint64
	reserveReleased bool
}

func NewSystemFileStore(paths Paths) (*FileStore, error) {
	serviceUser, err := user.Lookup("backupchief")
	if err != nil {
		return nil, fmt.Errorf("find backupchief service account: %w", err)
	}
	serviceGroup, err := user.LookupGroup("backupchief")
	if err != nil {
		return nil, fmt.Errorf("find backupchief service group: %w", err)
	}
	uid, err := strconv.Atoi(serviceUser.Uid)
	if err != nil {
		return nil, fmt.Errorf("parse backupchief uid: %w", err)
	}
	gid, err := strconv.Atoi(serviceGroup.Gid)
	if err != nil {
		return nil, fmt.Errorf("parse backupchief gid: %w", err)
	}
	return &FileStore{
		Paths:         paths,
		ServiceUID:    uid,
		ServiceGID:    gid,
		bootstrapMode: 0o640,
		spoolLimit:    SpoolBytesLimit,
		reserveBytes:  SpoolReserveBytes,
	}, nil
}

func NewUserFileStore(paths Paths) *FileStore {
	return &FileStore{
		Paths:         paths,
		ServiceUID:    -1,
		ServiceGID:    -1,
		bootstrapMode: 0o600,
		spoolLimit:    SpoolBytesLimit,
		reserveBytes:  SpoolReserveBytes,
	}
}

func NewTestFileStore(paths Paths) *FileStore {
	return &FileStore{
		Paths:      paths,
		ServiceUID: -1,
		ServiceGID: -1,
		spoolLimit: SpoolBytesLimit,
	}
}

func (store *FileStore) LoadBootstrap() (Bootstrap, error) {
	data, err := os.ReadFile(store.Paths.Identity)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Bootstrap{}, ErrManagedIdentityMissing
		}
		return Bootstrap{}, err
	}
	var document identityDocument
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return Bootstrap{}, fmt.Errorf("decode identity.json: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return Bootstrap{}, fmt.Errorf("decode identity.json: %w", err)
	}
	if !compatibleProtocolRevision(document.ProtocolRevision) {
		return Bootstrap{}, fmt.Errorf("managed identity protocol revision is invalid")
	}
	bootstrap := Bootstrap{
		Endpoint: document.Endpoint, ServerID: document.ServerID, Generation: document.Generation,
		Credential: document.Credential, Hostname: document.Hostname, SetupTokenHash: document.SetupTokenHash,
	}
	if err := validateBootstrap(bootstrap); err != nil {
		return Bootstrap{}, err
	}
	return bootstrap, nil
}

func (store *FileStore) SaveBootstrap(bootstrap Bootstrap) error {
	if err := validateBootstrap(bootstrap); err != nil {
		return err
	}
	uid, gid := 0, store.ServiceGID
	if store.ServiceUID < 0 {
		uid, gid = -1, -1
	}
	mode := store.bootstrapMode
	if mode == 0 {
		mode = 0o640
	}
	document := identityDocument{
		ProtocolRevision: ProtocolRevision,
		Endpoint:         bootstrap.Endpoint,
		ServerID:         bootstrap.ServerID,
		Generation:       bootstrap.Generation,
		Credential:       bootstrap.Credential,
		Hostname:         bootstrap.Hostname,
		SetupTokenHash:   bootstrap.SetupTokenHash,
	}
	return store.writeJSON(store.Paths.Identity, document, mode, uid, gid)
}

func (store *FileStore) LoadPending() (pendingEnrollment, error) {
	var pending pendingEnrollment
	if err := readJSON(store.Paths.Pending, &pending); err != nil {
		return pendingEnrollment{}, err
	}
	if !digestPattern.MatchString(pending.TokenHash) || !ulidPattern.MatchString(pending.AttemptID) {
		return pendingEnrollment{}, fmt.Errorf("pending setup journal is invalid")
	}
	if _, err := base64.RawURLEncoding.DecodeString(pending.Credential); err != nil {
		return pendingEnrollment{}, fmt.Errorf("pending credential is invalid")
	}
	if (pending.PreviousServerID == "") != (pending.PreviousGeneration == 0) {
		return pendingEnrollment{}, fmt.Errorf("pending previous identity is invalid")
	}
	if pending.PreviousServerID != "" && !ulidPattern.MatchString(pending.PreviousServerID) {
		return pendingEnrollment{}, fmt.Errorf("pending previous identity is invalid")
	}
	return pending, nil
}

func (store *FileStore) SavePending(pending pendingEnrollment) error {
	uid, gid := 0, 0
	if store.ServiceUID < 0 {
		uid, gid = -1, -1
	}
	return store.writeJSON(store.Paths.Pending, pending, 0o600, uid, gid)
}

func (store *FileStore) RemovePending() error {
	err := os.Remove(store.Paths.Pending)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (store *FileStore) LoadRuntimeState() (RuntimeState, error) {
	return store.loadRuntimeState()
}

func (store *FileStore) SaveRuntimeState(state RuntimeState) error {
	return store.saveRuntimeState(state)
}

func (store *FileStore) writeJSON(path string, value any, mode os.FileMode, uid, gid int) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode %s: %w", filepath.Base(path), err)
	}
	data = append(data, '\n')
	return writeAtomic(path, data, mode, uid, gid)
}

func readJSON(path string, target any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode %s: %w", filepath.Base(path), err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return fmt.Errorf("decode %s: %w", filepath.Base(path), err)
	}
	return nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return fmt.Errorf("multiple JSON values")
	}
	return err
}

func writeAtomic(path string, data []byte, mode os.FileMode, uid, gid int) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create %s: %w", directory, err)
	}

	temporary, err := os.CreateTemp(directory, "."+filepath.Base(path)+"-*")
	if err != nil {
		return fmt.Errorf("create temporary %s: %w", filepath.Base(path), err)
	}
	temporaryPath := temporary.Name()
	cleanup := func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}

	if err := temporary.Chmod(mode); err != nil {
		cleanup()
		return fmt.Errorf("set %s permissions: %w", filepath.Base(path), err)
	}
	if uid >= 0 && gid >= 0 {
		if err := temporary.Chown(uid, gid); err != nil {
			cleanup()
			return fmt.Errorf("set %s ownership: %w", filepath.Base(path), err)
		}
	}

	if _, err := temporary.Write(data); err != nil {
		cleanup()
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	if err := temporary.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync %s: %w", filepath.Base(path), err)
	}
	if err := temporary.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close %s: %w", filepath.Base(path), err)
	}

	if err := os.Rename(temporaryPath, path); err != nil {
		cleanup()
		return fmt.Errorf("replace %s: %w", filepath.Base(path), err)
	}

	// Sync the directory so the rename survives a crash after this function returns.
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open %s for sync: %w", directory, err)
	}
	defer directoryHandle.Close()
	if err := directoryHandle.Sync(); err != nil {
		return fmt.Errorf("sync %s: %w", directory, err)
	}

	return nil
}
