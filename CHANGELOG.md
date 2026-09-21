# Changelog

Notable changes to Backup Chief agent are documented here.

## [Unreleased]

### Added

- Reported whether Plesk is installed so the control plane can hide Plesk-specific setup on unrelated servers.
- Allowed MySQL and PostgreSQL jobs to read a server-local password file at inspection and backup time without sending the password to the control plane.
- Allowed file jobs to back up multiple selected files and directories beneath a shared root in one snapshot.
- Added a MySQL selection mode that includes every persistent database, including `mysql`, while excluding virtual system schemas.

### Changed

- Updated the agent protocol to 1.14 for Plesk capability detection, multi-path file sources, persistent MySQL database selection, and server-local database credential references.

## [0.14.1]

### Changed

- Adjusted the packaged agent CPU quota at boot to half of the machine's online processors, with a minimum of one core and a maximum of four, while retaining its lowered CPU scheduling priority.

## [0.14.0]

### Changed

- Limited the packaged agent service to one CPU core and lowered its CPU and I/O scheduling priority so backups, maintenance, replication, and child processes have less impact on host workloads.
- Limited Restic to one Go worker and one concurrent file read, and removed the filesystem progress pre-scan to reduce CPU and I/O work during backups.

## [0.13.0]

### Changed

- Updated the agent protocol to 1.12 so a successfully reported gap is acknowledged and cleared locally without clearing a newer gap that occurred concurrently.

### Fixed

- Reported a gap only when evidence was actually discarded or could not be retained, instead of flagging terminal and inventory events that remain durably queued for retry.
- Kept ordinary coalesced schedule occurrences from creating reporting-gap warnings while preserving atomic occurrence and gap state under real spool pressure.

## [0.12.1]

### Fixed

- Kept one generated run identity across agent update state, updater readiness, and terminal results, and repaired legacy mismatches during startup.
- Discarded expired unacknowledged manual commands before acknowledgement so they cannot block later work in the command queue.

## [0.12.0]

### Added

- Added SFTP storage destinations for primary and replica repositories, including backups, maintenance, replication, restores, and exports.
- Added password and Ed25519 authentication for SFTP, with pinned host-key verification and guarded connections through the bundled rclone transport.

### Changed

- Updated the agent protocol to 1.11 for SFTP storage configuration.

## [0.11.0]

### Added

- Added control-plane-triggered agent updates that wait for active backups, maintenance, source inspections, and replica transfers to finish before changing the installed package.
- Added a root-only systemd updater for APT, DNF, and YUM installations, with exact-version preflight checks, restart readiness verification, durable recovery, and automatic rollback when the updated agent does not become ready.
- Reported update availability and terminal update outcomes to the control plane so updates can be scheduled, monitored, and retried without granting package-manager privileges to the backup daemon.

### Changed

- Kept newly received commands and scheduled occurrences durable while an update is draining, then resumed them after the agent returned online.
- Updated the agent protocol to 1.10 for update capability discovery, update commands, acknowledgements, and structured results.

## [0.10.2]

### Fixed

- Resolved protected snapshots to each repository's local snapshot IDs before retention, preventing successful replica copies from being reported as missing.

## [0.10.1]

### Fixed

- Included symbolic link targets in non-recursive snapshot directory listings so file explorers can show where links point.

## [0.10.0]

### Added

- Added explicit initial replica synchronization on the agent. Provisioning replicas are copied and verified before they become available for ongoing replication, restore, export, or maintenance operations.
- Added per-repository maintenance results with attempt counts, bounded diagnostics, repository sizes, and snapshot evidence so partial failures are visible and retryable.

### Changed

- Applied forget, prune, metadata checks, data checks, and snapshot inventories to the primary repository first and then to every attached replica.
- Serialized replica copies and maintenance for each backup job, coalesced pending copy work, and retried transient repository failures with bounded backoff.
- Used logical backup-run tags for holds and targeted expiration so the same recovery point is selected even when replica snapshot IDs differ from the primary.
- Updated the agent protocol to 1.9 for provisioning replica configuration, replica synchronization commands, logical recovery-point identifiers, and per-repository maintenance results.

## [0.9.0]

### Added

- Added attached repository replicas for backup jobs. Snapshots from successful and partially successful backup runs are copied from the primary repository to each replica, with durable retries and replication results reported to the control plane.
- Added direct snapshot restores and portable ZIP exports from configured replica repositories.
- Bundled rclone for repository transfers between local and S3 storage, so replication does not require a separately installed rclone executable.

### Changed

- Updated the agent protocol to 1.8 for repository replication configuration and completion events.

## [0.8.0]

### Added

- Added direct snapshot restores into an empty or non-existing directory while keeping portable ZIP exports. File jobs can restore either their configured backup root or a selected directory directly into the target, and database jobs restore their uncompressed `.sql` artifact.

### Changed

- Updated the agent protocol to 1.7 to advertise direct snapshot restore support to the control plane.

## [0.7.2]

### Fixed

- Prevented per-database MySQL dumps from embedding server-wide GTID state by default, so independent database restores do not conflict. The agent detects whether the installed client supports the option and continues to honor an explicit custom GTID setting.

## [0.7.1]

### Fixed

- Fixed Amazon S3 repository connections through the guarded proxy by using the same region-specific dual-stack endpoint for Restic and the proxy.

## [0.7.0]

### Added

- Added table-level include and exclude selection for MySQL and PostgreSQL backups. Source inspections verify that selected tables exist and are accessible before a backup runs.
- Added database-level exclusions for MySQL and PostgreSQL backups.
- Reported each completed database snapshot while a multi-database backup is still running, making successful database artifacts available for download even when a later database fails.

### Changed

- Updated the agent protocol to 1.6 for filtered database backups and database snapshot progress events.

## [0.6.0]

### Added

- Added coordinated maintenance after scheduled backups. Retention and integrity checks can wait for the next successful backup, prune when its configured interval is due, and start a catch-up backup when maintenance overlaps the next schedule.
- Added manual expiration of selected snapshots while preserving the latest complete recovery point and other snapshots protected by the control plane. Expiring a database snapshot removes the complete backup-run group.

### Changed

- Updated the agent protocol to 1.5 for coordinated maintenance, protected snapshots, and targeted retention commands.
- Started newly received commands immediately instead of waiting for the next dispatch interval.

### Fixed

- Prevented maintenance failures from stale Restic locks by cleaning them up before maintenance and retrying cleanup when later operations encounter lock contention.

## [0.5.1]

### Fixed

- Fixed new retention runs skipping the Restic policy plan and reporting no snapshots to remove. Persisted maintenance plans now require a kind matching the operation before the agent resumes them.

## [0.5.0]

### Added

- Added ZIP exports for individual files, directories, and database artifacts from a selected snapshot. Exports stream directly from Restic, refuse to overwrite an existing output file, and advertise their availability to the control plane.

## [0.4.2]

### Added

- Reported the processed and stored bytes of each database's own snapshot alongside its backup artifact, so per-database sizes no longer collapse into the run total. Sent only to control planes on protocol revision 1.3.0 or later.

## [0.4.1]

### Added

- Added bounded, non-recursive directory listings for a path in a specific snapshot.

### Fixed

- Fixed PostgreSQL client detection, database discovery, and dumps on Debian and Ubuntu systems that use `pg_wrapper`. Discovery now reports SQL errors instead of treating them as an empty database selection.

## [0.4.0]

### Added

- Added PostgreSQL backup jobs using the system-installed `psql` and `pg_dump` tools. Jobs can back up selected databases or discover every accessible non-template database.
- Streamed each database directly into its own plain `.sql` file in Restic without writing the dump to temporary storage.
- Added PostgreSQL source inspections that verify credentials, discover accessible databases, and test `pg_dump` with a schema-only dump.
- Added startup capability probes for `psql` and `pg_dump`.

### Changed

- Included the server ID in the User-Agent for managed HTTP and WebSocket requests so the control plane can identify the originating server.

## [0.3.0]

### Added

- Added command-triggered repository snapshot inventories. Successful inventories report every snapshot and clear unresolved repository state.
- Added snapshot evidence to retention results, including a count, checksum, observation time, and the complete set of snapshot IDs sent in bounded event batches.

### Changed

- Updated the agent protocol to 1.2 for snapshot evidence and structured source-inspection failures. The agent omits these fields when it negotiates an older 1.x revision.
- Made source-inspection failures more useful by reporting the failed stage and sanitized technical detail to both the control plane and the agent log.
- Kept temporary MySQL credential files inside the agent's private state directory and removed them after each inspection or backup.

## [0.2.0]

### Added

- Added MySQL backup jobs using the system-installed `mysql` and `mysqldump` tools. Jobs can back up selected databases or discover every accessible non-system database.
- Streamed each database directly into its own uncompressed `.sql` file in Restic without writing the dump to temporary storage. Portable database names now produce readable filenames such as `synthetic_app.sql`.
- Added source inspection commands that verify file paths or MySQL credentials, test `mysqldump`, and report accessible databases.
- Added startup capability probes for supported backup types and required tools. The agent reports tool availability, paths, and versions with heartbeats.
- Added MySQL options for routines, events, and validated custom `mysqldump` flags.

### Changed

- Updated the agent protocol to 1.1 while keeping the `/agent/v1` endpoints. Agents can negotiate compatible 1.x revisions up or down while running, including after a control-plane rollback.
- Made configuration parsing forward-compatible with unsupported backup types and storage destinations so older agents can continue processing jobs they understand.
- Kept all per-database snapshots from one MySQL run together during retention by selecting one run anchor and expanding retention decisions to the complete group.
- Increased MySQL database and snapshot limits from 100 to 1000 per run.

## [0.1.0]

_Initial release._

[Unreleased]: https://github.com/chieftools/backupchief-agent/compare/v0.14.1...HEAD
[0.14.1]: https://github.com/chieftools/backupchief-agent/compare/v0.14.0...v0.14.1
[0.14.0]: https://github.com/chieftools/backupchief-agent/compare/v0.13.0...v0.14.0
[0.13.0]: https://github.com/chieftools/backupchief-agent/compare/v0.12.1...v0.13.0
[0.12.1]: https://github.com/chieftools/backupchief-agent/compare/v0.12.0...v0.12.1
[0.12.0]: https://github.com/chieftools/backupchief-agent/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/chieftools/backupchief-agent/compare/v0.10.2...v0.11.0
[0.10.2]: https://github.com/chieftools/backupchief-agent/compare/v0.10.1...v0.10.2
[0.10.1]: https://github.com/chieftools/backupchief-agent/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/chieftools/backupchief-agent/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/chieftools/backupchief-agent/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/chieftools/backupchief-agent/compare/v0.7.2...v0.8.0
[0.7.2]: https://github.com/chieftools/backupchief-agent/compare/v0.7.1...v0.7.2
[0.7.1]: https://github.com/chieftools/backupchief-agent/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/chieftools/backupchief-agent/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/chieftools/backupchief-agent/compare/v0.5.1...v0.6.0
[0.5.1]: https://github.com/chieftools/backupchief-agent/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/chieftools/backupchief-agent/compare/v0.4.2...v0.5.0
[0.4.2]: https://github.com/chieftools/backupchief-agent/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/chieftools/backupchief-agent/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/chieftools/backupchief-agent/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/chieftools/backupchief-agent/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/chieftools/backupchief-agent/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/chieftools/backupchief-agent/releases/tag/v0.1.0
