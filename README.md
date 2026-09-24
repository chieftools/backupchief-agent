# Backup Chief Agent

The Backup Chief Agent is a Go service for Linux that runs file backups and repository maintenance on the machine that holds the data. It reads the configured directories, invokes an embedded [Restic](https://restic.net/), and writes encrypted repositories directly to storage owned by the customer.

The optional [Backup Chief control plane](https://backup.chief.app/) manages servers, jobs, schedules, recovery material, history, and alerts. The same agent also runs standalone from a hand-written configuration when no control plane is required. Backup data never passes through the control plane.

> [!IMPORTANT]
> Independent recovery needs the repository password and the storage-provider credentials. Keep both outside the data they protect and test restores.

> [!CAUTION]
> Backup Chief is under active development. Keep the agent updated and review configuration changes before deployment.

## Features

### Backups

- Backs up one or more absolute directories per job into its own Restic repository.
- Runs jobs from a locally cached schedule, or on demand from the command line.
- Keeps schedules and pending results on disk, so a control-plane outage does not stop work that is already due.
- Supports Restic exclude patterns and single-filesystem traversal.
- Refuses destructive retention while a run outcome is unresolved.

### Storage and repositories

- Writes to S3-compatible storage, SFTP, or a local path.
- Produces standard Restic repositories that the stock CLI can read.
- Embeds a pinned, checksum-verified Restic for each supported architecture.
- Restricts storage traffic to the configured endpoint through a per-operation egress policy.

### Operations

- Runs scheduled forget, prune, metadata-check, and rotating data-check work.
- Journals outcomes, logs, snapshot IDs, and statistics before reporting them.
- Replays unreported results after connectivity returns.
- Applies a replacement configuration only after it validates.

## Installation

### Prerequisites for source builds

- Go 1.27 or later
- Linux on amd64 or arm64 for a managed or standalone deployment
- Root access to install the package, its service, and its state directories

### Build from source

```bash
# Clone the repository
git clone https://github.com/chieftools/backupchief-agent.git
cd backupchief-agent

# Build for current platform
go build -o backupchief .
```

### Quick install

```bash
curl -fsSL https://pkg.backup.chief.app/install.sh | sudo bash
```

Packages are built for amd64 and arm64 on Debian, Ubuntu, RHEL, CentOS, Rocky Linux, AlmaLinux, and Fedora families. The package stays inactive until the agent has either a managed or a standalone configuration.

### Upgrade and rollback

```bash
# Debian or Ubuntu
sudo apt-get update
sudo apt-get install --only-upgrade backupchief

# RHEL, CentOS, Rocky Linux, AlmaLinux, or Fedora
sudo dnf clean expire-cache --disablerepo='*' --enablerepo=backupchief
sudo dnf upgrade backupchief
```

The package transaction reloads systemd and restarts `backupchief.service` only when it was already active. An inactive installation stays inactive, so an ordinary upgrade needs no manual restart.

To roll back, install a saved earlier package through the same package manager. Preserve `/etc/backupchief/identity.json`, `/var/lib/backupchief`, and `/var/log/backupchief` so identity and pending work survive the change, and preserve `/etc/backupchief/config.json` in standalone mode.

Removing the package stops and disables the service but keeps managed identity, agent state, and logs. A DEB purge may remove the package-owned standalone configuration. Package scripts never recursively delete identity, state, logs, or customer repositories.

### Initial setup

Copy the one-time setup token from the Backup Chief control plane, then run:

```bash
sudo backupchief setup <token>
```

Setup stores the control-plane endpoint, server identity, and bearer credential in `/etc/backupchief/identity.json`, writes the last known good jobs and destinations to `/var/lib/backupchief/config.json`, then enables and starts `backupchief.service`.

Trigger a backup by its configuration key, or by a unique case-insensitive display name:

```bash
sudo backupchief backup <job>
```

A managed backup is queued through the control plane and the command waits for its result. Pressing Ctrl+C requests cancellation before the command exits.

## Run without a control plane

The agent runs in managed mode whenever `/etc/backupchief/identity.json` exists and ignores the standalone file. Without it, the agent reads `/etc/backupchief/config.json`, or the path given to the global `--config` option.

### JSON schema

The repository includes [config.schema.json](config.schema.json) for editor completion and validation. The public copy is published at `https://pkg.backup.chief.app/config.schema.json`.

### Example configuration

```json
{
  "$schema": "https://pkg.backup.chief.app/config.schema.json",
  "metadata": {
    "schema_version": 1
  },
  "destinations": {
    "storage_primary": {
      "driver": "local",
      "path": "/srv/backup-repositories"
    }
  },
  "jobs": {
    "job_documents": {
      "name": "Documents",
      "type": "file",
      "source": {
        "root": "/srv/documents",
        "excludes": ["/srv/documents/cache"]
      },
      "repository": {
        "destination": "storage_primary",
        "path": "documents/repository",
        "password": "replace-with-a-long-random-secret"
      },
      "schedule": "0 2 * * *"
    }
  }
}
```

Configuration keys use `job_` and `storage_` prefixes. Paths must be safe, schedules are fixed five-field UTC cron expressions with at least five minutes between occurrences, and duplicate fields are rejected. Additive fields unknown to the installed agent are ignored. Jobs with unsupported types, destinations with unsupported drivers, and jobs that depend on those destinations are skipped with warnings while supported jobs continue to run. The JSON Schema remains strict so editors and manual validation can catch typos in fields supported by the current release.

Every job declares a type; schema version 1 supports `file` and gives it a filesystem-specific `source` object. Retention and integrity settings have deterministic defaults when omitted. An `s3` destination needs a public HTTPS endpoint, a region, a bucket, an access key, and a secret key. An `sftp` destination needs a public host, port, username, normalized root path, one or more complete pinned host keys, and either password or Ed25519 authentication. Standalone configuration may contain the unencrypted PKCS#8 private key inline and must therefore remain readable only by the service account. An optional `host.name` sets the Restic host label; otherwise Restic uses the system hostname.

```json
{
  "driver": "sftp",
  "host": "archive.example.test",
  "port": 22,
  "username": "backup",
  "root_path": "/repositories",
  "host_keys": ["ssh-ed25519 <base64 public host key>"],
  "auth": {
    "method": "ed25519",
    "private_key": "-----BEGIN PRIVATE KEY-----\n<PKCS#8 key>\n-----END PRIVATE KEY-----\n"
  }
}
```

### Validate and start

Keep the file readable only by the service account, because it holds repository and provider credentials. Validate and initialize before starting the service:

```bash
sudo backupchief config validate
sudo backupchief repository init job_documents
sudo systemctl enable --now backupchief
```

The running agent checks the file every second, applies valid replacements, and keeps the last valid configuration when an edit is rejected. `backupchief backup <job>` runs a standalone job directly and waits for Restic to finish.

## Operations

Inspect the service with `systemctl status backupchief` and its output with `journalctl -u backupchief`.

| Path | Contents |
| --- | --- |
| `/etc/backupchief/identity.json` | Managed control-plane endpoint, server identity, and credential. |
| `/etc/backupchief/config.json` | Standalone configuration, when no managed identity exists. |
| `/var/lib/backupchief/config.json` | Last accepted managed configuration. |
| `/var/lib/backupchief/state.db` | Run journal, pending commands, and replay state. |
| `/var/lib/backupchief/run-logs` | Bounded run logs awaiting upload. |
| `/var/log/backupchief` | Service logs. |

The unit runs as the unprivileged `backupchief` user with `CAP_DAC_READ_SEARCH` so it can read protected files without full root, under `ProtectSystem=strict` with `/var/lib/backupchief` and `/var/log/backupchief` writable.

Agent state is durable because it may hold results the control plane has not accepted yet. Preserve it when replacing a machine.

## Recovery

Repositories stay compatible with the stock Restic CLI, so recovery does not depend on this agent or on Backup Chief:

```bash
export RESTIC_REPOSITORY='<repository location>'
export RESTIC_PASSWORD='<from your saved recovery material>'
restic snapshots
restic check
restic restore latest --target /absolute/restore/path
```

For an S3 repository, also set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from the separately saved storage-provider credentials.

## Architecture

### Components

| Component | Responsibility |
| --- | --- |
| CLI | Loads commands, flags, and process lifecycle. |
| Daemon | Runs the scheduler, command poller, and configuration watcher. |
| Scheduler | Evaluates cached UTC cron schedules and claims due occurrences. |
| Execution | Runs backup and maintenance work and records its outcome. |
| Journal | Persists runs, logs, and pending reports across restarts. |
| Client | Exchanges configuration, commands, events, results, and log chunks with the control plane. |
| Restic runner | Extracts the verified embedded Restic and invokes it with structured arguments. |
| Egress policy | Pins the validated storage authority for each operation. |

### Backup flow

1. The agent loads its managed or standalone configuration and caches it.
2. The scheduler claims a due occurrence, or a manual command requests a run.
3. The runner extracts the verified embedded Restic for the host architecture.
4. Restic reads the configured root and writes encrypted data directly to the destination.
5. The agent journals the outcome, log, snapshot IDs, and statistics.
6. In managed mode the agent reports the journaled result, replaying it until the control plane accepts it.

The control plane is not in the backup data path, so a temporary outage does not stop schedules the agent has already accepted.

## Development

### Run tests

```bash
go test ./...
```

The `dev` command builds behind the `devtools` tag and runs an unprivileged local agent against a development control plane:

```bash
./bin/dev.sh setup <token>
./bin/dev.sh run
```

See [CHANGELOG.md](CHANGELOG.md) for release notes.

## Security vulnerabilities

Report vulnerabilities privately through [GitHub Security Advisories](https://github.com/chieftools/backupchief-agent/security/advisories/new). Backup Chief does not currently run a bug bounty program.

## License

The Backup Chief Agent is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for the license text.
