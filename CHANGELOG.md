# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.5.0]

### Fixed

- Async replication pool is now a permanent child of the application
  supervision tree. Previously it was started lazily and linked to whichever
  process triggered the first replicated write — when that process was a
  transient `:erpc` executor (e.g. a forwarded write), its exit killed the
  pool and silently dropped in-flight replications.
- Partition ownership is now reconciled after membership changes: nodes that
  gain responsibility for partitions they did not hold before pull the records
  from previous holders, instead of leaving them stranded on old owners.
- `Bootstrap.stop/0` no longer raises when the supervision tree is already
  down; it logs and returns `:ok`.

### Changed

- Health monitor telemetry emissions are documented as optional (`:telemetry`
  dependency not required).
- Expanded distributed guide: corrected health status values, NodeMonitor
  configuration examples, `DistributedStore` helper signatures, and documented
  ownership reconciliation on node join/leave.

### Added

- Test coverage tooling (`tools/coverage/uncovered.exs`) and a coverage round
  five suite; total line coverage raised from ~94.4% to ~95.7% with Stack,
  Storage, Metrics, Stats and Cluster.Manager at 100%.

## [1.4.0] - 2026-05-03

### Changed

- Improve performance.

## [1.3.0] - 2026-04-18

### Added

- Improve `KeyValue` module.

### Changed

- Refactor internal code.

## [1.2.1] - 2026-04-05

### Changed

- Update docs and add examples.

## [1.2.0] - 2026-04-04

### Changed

- Improve performance.

## [1.0.0] - 2026-03-12

### Changed

- Improve distributed mode.
- First stable release.

## [0.11.0] - 2026-03-09

### Changed

- Update distributed APIs.

## [0.10.1] - 2026-03-07

### Fixed

- Add 3-phase commits for other modules in distributed mode.

## [0.10.0] - 2026-03-07

### Added

- 3-phase commits for distributed mode.
- Minor improvements.

### Fixed

- Bugs for distributed mode.

## [0.9.0] - 2026-03-06

### Changed

- Update docs.

## [0.8.0] - 2026-03-06

### Added

- Support for distributed cache.

## [0.7.1] - 2026-03-05

### Fixed

- Bug fixes.

## [0.7.0] - 2026-03-05

### Added

- Struct storage.

## [0.6.1]

### Added

- More APIs for key/value, stack, and queue.

## [0.6.0]

## [0.5.2]

### Fixed

- Duplicate Supervisor name vs other app.

## [0.5.1]

### Added

- Lazy write to improve write performance.

### Fixed

- Unit tests & lazy write.

## [0.5.1-dev]

## [0.4.1]

### Changed

- Fix doc & performance script, add profile script.

## [0.4.0-dev2]

### Added

- Script test for benchmark.

## [0.4.0-dev]

### Removed

- Some unnecessary code, add test performance tool.

## [0.3.1-dev]

### Added

- Unit tests & `delete_all` function.

## [0.3.0-dev]

### Added

- Initial public release with documentation for publishing the package.

[Unreleased]: https://github.com/ohhi-vn/super_cache/compare/v1.4.0...HEAD
[1.4.0]: https://github.com/ohhi-vn/super_cache/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/ohhi-vn/super_cache/compare/v1.2.1...v1.3.0
[1.2.1]: https://github.com/ohhi-vn/super_cache/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/ohhi-vn/super_cache/compare/v1.0.0...v1.2.0
[1.0.0]: https://github.com/ohhi-vn/super_cache/compare/v0.11.0...v1.0.0
[0.11.0]: https://github.com/ohhi-vn/super_cache/compare/v0.10.1...v0.11.0
[0.10.1]: https://github.com/ohhi-vn/super_cache/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/ohhi-vn/super_cache/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/ohhi-vn/super_cache/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/ohhi-vn/super_cache/compare/v0.7.1...v0.8.0
[0.7.1]: https://github.com/ohhi-vn/super_cache/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/ohhi-vn/super_cache/compare/v0.6.1...v0.7.0
