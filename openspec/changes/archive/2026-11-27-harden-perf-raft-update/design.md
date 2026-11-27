# Design

## Context

See proposal.md Why. Current state: `mix.lock` pins `raft_ex 0.7.0`, but `SuperCache.Cluster.Raft.Config`/`System` only know the 0.4-era keys (data_dir, command timeout, WAL sync/size, election/heartbeat, segment max entries/size). `RaftEx.App.System.default_config/0` now also defines `wal_max_batch_size`, `segment_compute_checksums`, `segment_storage`, `log_window_size`, compaction thresholds/intervals, `max_pipeline_count`, `max_append_entries_rpc_batch_size`, `max_pending_commands`, `command_batch_linger_ms`, `snapshot_chunk_size`, plus app-env overrides and server config persistence. `Adapter` exposes only `find_leader/members/add/promote/demote/remove/start/stop/start_cluster/start_or_restart/force_delete/process_command/local|leader|consistent_query/key_metrics/member_overview`. Reconciler's no-leader and catch-up-timeout paths call `form_cluster/3`, which stops servers and restarts with an empty log (CHANGELOG 1.9.0 notes data loss if fired after committed writes).

## Goals / Non-Goals

**Goals:**
- Pass through 0.5–0.7 tuning with validated defaults without breaking existing config.
- Treat durable-write failures and pending-queue backpressure as normal documented errors.
- Remove log-discarding repair after committed writes; keep fast first-formation path.
- Surface lag, server/cluster metrics, and effective tuning via existing stats/health.
- Keep single-commit-per-partition-batch and existing read semantics.

**Non-Goals:**
- No new replication mode, wire protocol, or on-disk layout migration.
- No attempt to make `:raft` as fast as `:strong`; goal is bounded improvement + documented guidance.
- No rewrite of storage engine or public APIs.

## Decisions

- **Config passthrough with allowlist mapping:** Extend `Raft.Config` defaults/validation for `max_pending_commands`, `command_batch_linger_ms`, `max_pipeline_count`, `max_append_entries_rpc_batch_size`, `snapshot_chunk_size`, `segment_storage`, `segment_compute_checksums`, `compaction_*`, `wal_max_batch_size`; map 1:1 into `System.system_config/0` on top of `RaftEx.App.System.default_config/0`. Alternative (raw map merge) rejected: would silently forward typos and skip startup validation required by `raft-replication`.
- **Respect engine app-env precedence:** `RaftEx` reads `Application.get_env(:raft_ex, key)` over compiled defaults; SuperCache sets explicit values from its own `:super_cache, :raft` config so behavior is deterministic per node, and documents that direct `:raft_ex` env is an escape hatch, not the supported path.
- **Adapter stays a thin boundary:** Add `server_metrics/cluster_metrics` (and `delete_cluster` only if needed for tests) delegating to backend; keep `:raft_adapter` mock injection. New Raft error/backpressure shapes pass through unchanged into `Raft.submit/3` normalization. Alternative (leaking `RaftEx` calls outside `Raft.*`) rejected to preserve the single-boundary rule.
- **Submit path hardening:** Keep bounded leader-retry with deadline and never resubmit ambiguous timeouts; map `:backpressure`/`{:error, :not_appended, _}`/durable-write errors to `Cluster.Error` via `Rpc.normalize/2` and count as `raft_failures`. Alternative (retry on timeout) rejected: risks duplicate commits.
- **Safe reconciliation:** Keep `Reconciler` as sole owner of `prepare_member/2`. Departure and crash paths (no leader found, or a gone leader process) first wait up to 5s for an in-progress election before reforming, so a transient election is never destroyed by reformation; when no leader emerges, reform via restart-recovery (`start_or_restart_cluster`) so surviving members keep their persisted logs and terms instead of starting empty, and the group converges from the current partition map. Only the catch-up-failure path (`:catch_up_timeout`/`:not_caught_up` during join) is gated by `safe_form_cluster`, which refuses reformation and reports `{:error, :committed_data_present}` when any member reports a commit/applied index or the local partition holds records. Tolerate `:already_member`/`:already_voter`, retry promotion to deadline, propagate other join failures.
- **Observability reuse:** Extend `Group.status/1` metric allowlist with `replication_lag`, `last_applied`-adjacent gauges, and server/cluster metric calls where cheap (bounded timeouts, rescue to `%{}`); feed `Stats`/`HealthMonitor` without new top-level APIs. Effective tuning exposed from `Config.all/0`.
- **Perf within invariants:** Keep ETS single-insert-per-partition-batch, `:ets.select_delete` pruning, `persistent_term` struct-metadata caching; reduce per-write overhead by avoiding extra `members/key_metrics` calls on the hot path (only on leader-failover/reconcile), coalescing commit broadcasts via engine batching, and bounding health/handoff batch sizes. Benchmark with `benchmarks/distributed_benchmark.exs` (`BENCH_RF`) before/after.

## Risks / Trade-offs

- [Risk] Tuning cannot close the 8–50× `:raft` vs `:strong` gap → Mitigation: document when to choose each mode; set conservative defaults (pipelining on, modest batch sizes).
- [Risk] Aggressive compaction discards entries needed for ETS rebuild → Mitigation: retain replay-required entries; validate thresholds; default to upstream-safe values.
- [Risk] New metrics calls add latency on stats path → Mitigation: short timeouts, rescue-to-empty, no hot-path calls.
- [Risk] Stricter validation rejects previously accepted configs → Mitigation: defaults preserve 0.4 behavior; only new keys are additive.
- **Known limitation:** forced reformation after a leaderless departure depends on upstream `raft_ex` formation electing promptly; on slow or heavily loaded hosts the group may need several reconcile rounds before a leader emerges. The catch-up gate never discards committed log, and reformation recovers survivor logs instead of wiping them, so repeated attempts converge without data loss once the host can elect.

## Migration Plan

- Deploy: additive optional `config :super_cache, :raft` keys; no data migration; existing data_dirs reused (0.7 persists server config automatically).
- Rollback: unset new keys to restore 0.4-equivalent behavior; no log-format downgrade needed (engine handles recovery).
- Verification: `mix precommit`, `mix test --exclude cluster`, opt-in `mix test.cluster`, raft benchmark comparison, stats/health inspection.

## Open Questions

- Exact default values for pipeline/batch/compaction pending benchmark numbers on the target hardware.
- Whether to enable `segment_storage` by default or keep it opt-in for large-log deployments.
