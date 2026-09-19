package repository

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

type destinationDocument struct {
	Driver    Driver              `json:"driver"`
	Path      string              `json:"path,omitempty"`
	Endpoint  string              `json:"endpoint,omitempty"`
	Region    string              `json:"region,omitempty"`
	Bucket    string              `json:"bucket,omitempty"`
	Prefix    string              `json:"prefix,omitempty"`
	AccessKey string              `json:"access_key,omitempty"`
	SecretKey string              `json:"secret_key,omitempty"`
	Host      string              `json:"host,omitempty"`
	Port      uint16              `json:"port,omitempty"`
	Username  string              `json:"username,omitempty"`
	RootPath  string              `json:"root_path,omitempty"`
	HostKeys  []string            `json:"host_keys,omitempty"`
	Auth      *authenticationWire `json:"auth,omitempty"`
}

type connectionDocument struct {
	Driver    Driver              `json:"driver"`
	Path      string              `json:"path,omitempty"`
	Endpoint  string              `json:"endpoint,omitempty"`
	Bucket    string              `json:"bucket,omitempty"`
	Prefix    string              `json:"prefix,omitempty"`
	Region    string              `json:"region,omitempty"`
	AccessKey string              `json:"access_key,omitempty"`
	SecretKey string              `json:"secret_key,omitempty"`
	Host      string              `json:"host,omitempty"`
	Port      uint16              `json:"port,omitempty"`
	Username  string              `json:"username,omitempty"`
	HostKeys  []string            `json:"host_keys,omitempty"`
	Auth      *authenticationWire `json:"auth,omitempty"`
}

type authenticationWire struct {
	Method     string `json:"method"`
	Password   string `json:"password,omitempty"`
	PrivateKey string `json:"private_key,omitempty"`
}

// DecodeDestination tolerates additive fields so managed configurations remain forward-compatible.
func DecodeDestination(data []byte) (Destination, error) {
	var document destinationDocument
	if err := json.Unmarshal(data, &document); err != nil {
		return Destination{}, err
	}

	destination, err := destinationFromDocument(document)
	if err != nil {
		return Destination{}, err
	}

	return destination, destination.Validate()
}

func (destination Destination) MarshalJSON() ([]byte, error) {
	document, err := destination.document()
	if err != nil {
		return nil, err
	}

	return json.Marshal(document)
}

func (destination *Destination) UnmarshalJSON(data []byte) error {
	decoded, err := DecodeDestination(data)
	if err != nil {
		return err
	}

	*destination = decoded
	return nil
}

func (destination Destination) document() (destinationDocument, error) {
	switch value := destination.backend.(type) {
	case LocalDestination:
		return destinationDocument{Driver: DriverLocal, Path: value.Root}, nil
	case S3Destination:
		return destinationDocument{
			Driver: DriverS3, Endpoint: value.Endpoint, Region: value.Region, Bucket: value.Bucket,
			Prefix: value.Prefix, AccessKey: value.AccessKey, SecretKey: value.SecretKey,
		}, nil
	case SFTPDestination:
		auth, err := authenticationDocument(value.Authentication)
		if err != nil {
			return destinationDocument{}, err
		}

		return destinationDocument{
			Driver: DriverSFTP, Host: value.Host, Port: value.Port, Username: value.Username,
			RootPath: value.RootPath, HostKeys: append([]string(nil), value.HostKeys...), Auth: &auth,
		}, nil
	default:
		return destinationDocument{}, errors.New("driver is invalid")
	}
}

func destinationFromDocument(document destinationDocument) (Destination, error) {
	switch document.Driver {
	case DriverLocal:
		if document.Endpoint != "" || document.Region != "" || document.Bucket != "" || document.Prefix != "" || document.AccessKey != "" || document.SecretKey != "" || hasSFTPDocument(document) {
			return Destination{}, errors.New("local settings are invalid")
		}

		return NewLocalDestination(document.Path), nil
	case DriverS3:
		if document.Path != "" || hasSFTPDocument(document) {
			return Destination{}, errors.New("S3 settings are invalid")
		}

		return NewS3Destination(S3Destination{
			Endpoint: document.Endpoint, Region: document.Region, Bucket: document.Bucket, Prefix: document.Prefix,
			AccessKey: document.AccessKey, SecretKey: document.SecretKey,
		}), nil
	case DriverSFTP:
		if document.Path != "" || document.Endpoint != "" || document.Region != "" || document.Bucket != "" || document.Prefix != "" || document.AccessKey != "" || document.SecretKey != "" || document.Auth == nil {
			return Destination{}, errors.New("SFTP settings are invalid")
		}
		auth, err := authenticationFromDocument(*document.Auth)
		if err != nil {
			return Destination{}, err
		}

		return NewSFTPDestination(SFTPDestination{
			Host: document.Host, Port: document.Port, Username: document.Username, RootPath: document.RootPath,
			HostKeys: document.HostKeys, Authentication: auth,
		}), nil
	default:
		return Destination{}, errors.New("driver is invalid")
	}
}

func hasSFTPDocument(document destinationDocument) bool {
	return document.Host != "" || document.Port != 0 || document.Username != "" || document.RootPath != "" || len(document.HostKeys) != 0 || document.Auth != nil
}

func (connection Connection) MarshalJSON() ([]byte, error) {
	document, err := connection.document()
	if err != nil {
		return nil, err
	}

	return json.Marshal(document)
}

func (connection *Connection) UnmarshalJSON(data []byte) error {
	// Helper request payloads are a trust boundary, so unknown fields must fail closed.
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	var document connectionDocument
	if err := decoder.Decode(&document); err != nil {
		return err
	}

	decoded, err := connectionFromDocument(document)
	if err != nil {
		return err
	}

	*connection = decoded
	return nil
}

func (connection Connection) document() (connectionDocument, error) {
	switch value := connection.backend.(type) {
	case LocalConnection:
		return connectionDocument{Driver: DriverLocal, Path: value.Path}, nil
	case S3Connection:
		return connectionDocument{
			Driver: DriverS3, Endpoint: value.Endpoint, Bucket: value.Bucket, Prefix: value.Prefix,
			Region: value.Region, AccessKey: value.AccessKey, SecretKey: value.SecretKey,
		}, nil
	case SFTPConnection:
		auth, err := authenticationDocument(value.Authentication)
		if err != nil {
			return connectionDocument{}, err
		}

		return connectionDocument{
			Driver: DriverSFTP, Path: value.Path, Host: value.Host, Port: value.Port, Username: value.Username,
			HostKeys: append([]string(nil), value.HostKeys...), Auth: &auth,
		}, nil
	default:
		return connectionDocument{}, errors.New("unsupported repository driver")
	}
}

func connectionFromDocument(document connectionDocument) (Connection, error) {
	switch document.Driver {
	case DriverLocal:
		if document.Endpoint != "" || document.Bucket != "" || document.Prefix != "" || document.Region != "" || document.AccessKey != "" || document.SecretKey != "" || hasSFTPConnectionDocument(document) {
			return Connection{}, errors.New("invalid local connection")
		}

		return NewLocalConnection(document.Path), nil
	case DriverS3:
		if document.Path != "" || hasSFTPConnectionDocument(document) {
			return Connection{}, errors.New("invalid S3 connection")
		}

		return NewS3Connection(S3Connection{
			Endpoint: document.Endpoint, Bucket: document.Bucket, Prefix: document.Prefix, Region: document.Region,
			AccessKey: document.AccessKey, SecretKey: document.SecretKey,
		}), nil
	case DriverSFTP:
		if document.Endpoint != "" || document.Bucket != "" || document.Prefix != "" || document.Region != "" || document.AccessKey != "" || document.SecretKey != "" || document.Auth == nil {
			return Connection{}, errors.New("invalid SFTP connection")
		}
		auth, err := authenticationFromDocument(*document.Auth)
		if err != nil {
			return Connection{}, err
		}

		return NewSFTPConnection(SFTPConnection{
			Host: document.Host, Port: document.Port, Username: document.Username, Path: document.Path,
			HostKeys: document.HostKeys, Authentication: auth,
		}), nil
	default:
		return Connection{}, fmt.Errorf("unsupported repository driver %q", document.Driver)
	}
}

func hasSFTPConnectionDocument(document connectionDocument) bool {
	return document.Host != "" || document.Port != 0 || document.Username != "" || len(document.HostKeys) != 0 || document.Auth != nil
}

func authenticationDocument(authentication SFTPAuthentication) (authenticationWire, error) {
	switch authentication.Method() {
	case "password":
		return authenticationWire{Method: "password", Password: authentication.Secret()}, nil
	case "ed25519":
		return authenticationWire{Method: "ed25519", PrivateKey: authentication.Secret()}, nil
	default:
		return authenticationWire{}, errors.New("SFTP authentication method is invalid")
	}
}

func authenticationFromDocument(document authenticationWire) (SFTPAuthentication, error) {
	switch document.Method {
	case "password":
		if document.PrivateKey != "" {
			return SFTPAuthentication{}, errors.New("SFTP password authentication is invalid")
		}

		return PasswordAuthentication(document.Password), nil
	case "ed25519":
		if document.Password != "" {
			return SFTPAuthentication{}, errors.New("SFTP Ed25519 authentication is invalid")
		}

		return Ed25519Authentication(document.PrivateKey), nil
	default:
		return SFTPAuthentication{}, errors.New("SFTP authentication method is invalid")
	}
}
