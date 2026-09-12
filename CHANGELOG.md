# Changelog

Notable changes to Backup Chief agent are documented here.

## [Unreleased]

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

[Unreleased]: https://github.com/chieftools/backupchief-agent/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/chieftools/backupchief-agent/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/chieftools/backupchief-agent/releases/tag/v0.1.0
