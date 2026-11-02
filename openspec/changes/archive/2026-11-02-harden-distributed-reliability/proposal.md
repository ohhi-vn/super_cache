# Proposal

## Why

Distributed mode is documented as production-ready, but the current implementation has several reliability gaps that can make writes appear successful when replicas never applied them, can hang or leak work under partial failure, and can report misleading cluster health. The strong-consistency path is also split between two competing implementations (3PC in the router/helpers and WAL in the replicator), so the documented guarantee does not match the code that actually runs.

This change hardens the distributed write/read paths and membership layer before those gaps become production incidents.

## What Changes

- **Unify strong replication on one path.** Route `:strong` writes through a single WAL-based implementation, including `SuperCache`, `KeyValue`, `Queue`, `Stack`, and `Struct`; remove the router/helper bypass that currently selects 3PC instead.
- **Make replication failures visible and correct.** Propagate sync/strong replication errors to callers, normalize remote failures into a documented exception, fix quorum accounting to include the primary, deduplicate acknowledgements, and reject duplicate/partial quorum results.
- **Replace ad-hoc quorum waits.** Introduce one bounded quorum helper that performs early termination, enforces a deadline, kills remaining tasks, handles `nil` results, and is used by sync writes, 3PC (if retained), and quorum reads.
- **Fix async replication lifecycle.** Stop using `Task.Supervisor.async_nolink` for fire-and-forget work without consuming its result; use a supervised fire-and-forget worker, record failures, and add bounded retry/backoff without blocking the caller.
- **Harden partition transfer and ownership reconciliation.** Stream partition snapshots instead of materializing full tables, report accurate transferred/failed counts, select a single deterministic donor per partition, retry transient failures, and avoid duplicate concurrent syncs.
- **Correct health and membership reporting.** Run health checks against the node that owns the data, make replication-lag probes unique and representative, bound each health cycle by a deadline, and record timeouts as unhealthy/unknown rather than silently dropping them.
- **Remove hot-path GenServer hops.** Promote `:replication_mode`, `:replication_factor`, and `:started` to fast config keys so every distributed write does not call `Config` through a GenServer.
- **Preserve the public API.** No public function signatures or success return shapes change. Failure paths that previously returned success while dropping replication errors now raise the documented exception (bang APIs) or return `{:error, exception}` (non-bang APIs).
- **Add regression coverage and benchmarks.** Cover quorum edge cases, node failure, WAL timeout/recovery, error propagation, health checks, and partition transfer; record before/after benchmarks for local and distributed hot paths.

## Capabilities

### New Capabilities

- `distributed-writes`: replication modes, quorum semantics, strong-write recovery, idempotency/retry boundaries, and error propagation for distributed writes.
- `distributed-reads`: primary/quorum read semantics, failover behavior, read-your-writes routing, and failure reporting for distributed reads.
- `cluster-membership`: node discovery, partition assignment, ownership handoff, health reporting, and membership-change observability.

### Modified Capabilities

<!-- No existing specs in this project. -->

## Impact

- **Code**: `lib/cluster/{router,replicator,wal,three_phase_commit,distributed_helpers,manager,health_monitor,node_monitor,metrics}.ex`, `lib/app/config.ex`, `lib/api/{super_cache,key_value,queue,stack,struct}.ex`, and `lib/cluster/cluster_bootstrap.ex`.
- **Public behavior**: no API shape changes; documented failure contracts become true. Strong-mode callers may now see an error instead of a false success when quorum is not reached. `:strong` is consistently WAL-based; 3PC remains only as a compatibility/internal module unless removed in a follow-up.
- **Operations**: health checks may report `:degraded`/`:unhealthy` in cases that were previously silently ignored; new metrics expose replication and handoff failures.
- **Performance**: removes per-write `Config` GenServer calls, bounds quorum waits, streams partition transfers, and reduces duplicate ownership sync traffic. Existing benchmarks must not regress; new distributed benchmarks establish a baseline.
- **Dependencies**: no new runtime dependencies. `:telemetry` remains optional.
