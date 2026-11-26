# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.8.0]

### Added

- Advisory Raft topology diagnostic: with `replication_mode: :raft`, compares
  the live node count with `replication_factor + 1` on every membership
  change, logs a warning on the transition into `:underprovisioned`, and
  exposes `topology` in `SuperCache.cluster_stats().raft` and local health.
- `raft.groups` is preserved; `raft.group_details` now reports each tracked
  group's partition, running state, role, leader, member count, and members.
- Bounded leader retry in `SuperCache.Cluster.Raft.submit/3` for
  leader-placement failures; ambiguous timeouts are never resubmitted.
- `Raft.Group.prepare_member/2` rebuilds a joining member from the committed
  Raft log (discarding stale local server state, ETS partition, and
  applied-index watermark) instead of accepting an ETS handoff.
- `Applied.applied?/2` and `Applied.record/2` so the watermark advances only
  after successful application; committed-but-unapplied commands are reported
  as failures.
- `raft_ex` 0.4.0 integration: role-aware Raft membership (new members join as
  `:promotable` and are promoted after catch-up, departing members are demoted
  before removal), graceful leadership transfer on planned primary moves with
  fallback to the normal election path, and Raft-native local/leader/consistent
  query paths with an opt-in `:consistent` read mode.
- Snapshot metadata now checkpoints the applied-index watermark so replay stays
  idempotent across restarts and ETS recreation.
- Raft stats and group details include engine metrics (term, commit/applied
  index, state) and member roles; leadership-transfer and query failures are
  counted as Raft failures.

### Changed

- **BREAKING dependency**: `raft_ex` is now `~> 0.4.0` (was `~> 0.3.1`).
  Upstream renamed internal predicate helpers and moved default-system startup
  to the `:raft_ex` application environment; SuperCache starts its own named
  system and has no call sites for the renamed helpers. New optional settings
  `election_timeout_min`, `election_timeout_max`, `heartbeat_interval`,
  `segment_max_entries`, and `segment_max_size_bytes` are validated at bootstrap.
- Log entries needed to rebuild ETS partitions are retained: snapshot
  checkpoints carry metadata only, so compaction must not discard committed
  entries that a restart still needs to replay.

- In `:raft` mode, `Manager.reconcile_ownership/2` and `sync_to_node/1` no
  longer push ETS partitions; members rebuild from the Raft log.
- `Raft.Machine.apply/3` propagates apply failures instead of reporting
  success.

### Notes

- Running `replication_factor: N` with `N + 1` nodes is recommended for a
  spare; single-node and smaller clusters remain valid.
- Log entries needed for restart replay are retained: join time is proportional
  to the retained Raft log.

## [1.7.0]

### Added

- Opt-in `replication_mode: :raft` backed by `raft_ex`: one Raft cluster per
  partition, committed commands applied to ETS by a state machine, leader
  routing, normalized `SuperCache.Cluster.Error` failures, Raft group
  lifecycle tied to bootstrap/membership, and `raft` status plus
  `raft_failures` in cluster statistics and health reports.
- Optional `config :super_cache, :raft` settings (`data_dir`,
  `command_timeout_ms`, `wal_sync_method`, `wal_max_size_bytes`).

## [1.6.0]

### Changed

- `KeyValue.count/2`, `keys/2`, and `values/2` now compute results with ETS
  count and field projections instead of copying full key-value records into
  the caller.
- Distributed `KeyValue.remove_batch/2` removes all keys for a partition in
  one routed operation instead of issuing one remote call per key.
- Partition and replica batch writes insert the whole partition batch with a
  single `Storage.put/2` call rather than one insert per record.
- `lazy_put/1` buffer streams flush drained batches through
  `SuperCache.put_batch!/1` and deliver items in FIFO order per scheduler
  queue.
- Expired read-your-writes entries are pruned with `:ets.select_delete/2`
  instead of scanning the table into the caller.
- `Struct` caches resolved key-field metadata in `:persistent_term` after
  `init/2` (cleared on stop), so repeated struct operations avoid ETS and
  `:erpc` lookups.
- `mix dialyzer` now exits cleanly: the PLT includes `:logger`,
  `Storage.MatchSpec` types accept non-tuple bodies, OTP 29
  `:erpc.multicall/5` results are matched correctly, unreachable clauses are
  removed, the optional Telemetry call is guarded, and the benchmark Mix task
  uses current compiled match-spec APIs. No runtime behavior changes.

### Fixed

- `Stack` operations now bound contention spin-waiting like `Queue`, so a
  held or abandoned lock returns the documented default instead of spinning
  forever.
- Concurrent queue and stack initialization serializes initialization and
  counter updates, so a concurrent writer can no longer reset an active
  counter to zero.
- Queue and stack mutations release their lock on error and reclaim stale
  locks left behind by terminated or timed-out processes.
- `Storage.get_by_match/2` now extracts capture variables from nested tuples
  and returns bindings in numeric variable order (`:"$1"`, `:"$2"`, ...).
- The read-your-writes tracking table is owned by the permanent
  `SuperCache.Cluster.WAL.Store` process, so short-lived writers no longer
  destroy tracking state and concurrent first writes cannot race on table
  creation.
- Distributed `Struct.remove/1` executes on the primary partition owner
  instead of pre-checking a possibly stale local replica.
- Duplicate acknowledgements from the same replica no longer advance WAL
  quorum counts.

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
- Sync and strong write failures now reach callers through every public write
  surface. `Queue`, `Stack`, and `Struct` distributed writes no longer report
  success after `DistributedHelpers.apply_write/3` fails, and forwarded write
  failures are normalized to `SuperCache.Cluster.Error`.
- Read failures no longer surface as empty results: primary and quorum reads
  fall back only when the local node holds a replica, otherwise bang APIs
  raise and non-bang APIs return `{:error, exception}`.
- Health checks run against the owning node only. Remote partition counts come
  from the remote node, aggregation is bounded, and probes are unique and
  cleaned up after failures.
- Partition handoff is single-donor and coalesced, streams bounded batches,
  and reports exact transferred/failed counts.

### Changed

- Health monitor telemetry emissions are documented as optional (`:telemetry`
  dependency not required).
- Expanded distributed guide: corrected health status values, NodeMonitor
  configuration examples, `DistributedStore` helper signatures, and documented
  ownership reconciliation on node join/leave.
- Documented the write-failure contract change: sync/strong writes that cannot
  reach quorum now raise or return an error instead of silently succeeding.
- Async replication documentation now covers retry limits, backoff, and the
  `async_failures`/`async_drops` counters.

### Added

- Reliability counters for membership changes, handoff/transfer failures, and
  the existing replication/read/health failure paths, exposed through
  `SuperCache.Cluster.Stats.reliability/0`.
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

[Unreleased]: https://github.com/ohhi-vn/super_cache/compare/v1.6.0...HEAD
[1.6.0]: https://github.com/ohhi-vn/super_cache/compare/v1.5.0...v1.6.0
[1.5.0]: https://github.com/ohhi-vn/super_cache/compare/v1.4.0...v1.5.0
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
