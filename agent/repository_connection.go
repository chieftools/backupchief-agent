package agent

import "github.com/chieftools/backupchief-agent/restic"

func (connection RepositoryConnection) Restic() restic.Connection {
	resticConnection := restic.Connection{
		Driver:    connection.Driver,
		Path:      connection.Path,
		Endpoint:  connection.Endpoint,
		Bucket:    connection.Bucket,
		Prefix:    connection.Prefix,
		Region:    connection.Region,
		AccessKey: connection.AccessKey,
		SecretKey: connection.SecretKey,
		Host:      connection.Host,
		Port:      connection.Port,
		Username:  connection.Username,
		HostKeys:  append([]string(nil), connection.HostKeys...),
	}
	if connection.Auth != nil {
		resticConnection.SFTPPassword = connection.Auth.Password
		resticConnection.SFTPPrivateKey = connection.Auth.PrivateKey
	}

	return resticConnection
}
