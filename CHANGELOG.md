# Changelog

Notable changes to Backup Chief agent are documented here.

## [Unreleased]

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

[Unreleased]: https://github.com/chieftools/backupchief-agent/compare/v0.9.0...HEAD
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
