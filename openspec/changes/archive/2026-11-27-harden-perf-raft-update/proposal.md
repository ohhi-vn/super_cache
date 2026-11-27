# Proposal

## Why

`raft_ex` moved from 0.4.0 to 0.7.0 with correctness fixes (tick routing, commit safety, reply routing, durable-write errors), throughput features (command/replication batching, pipelined catch-up, auto-compaction, segment storage), and new tuning/observability surface — while `mix.lock` already pins 0.7.0, SuperCache still only configures the 0.4-era knobs and does not handle the new error/backpressure contract. At the same time the 1.9.0 notes admit the Raft membership-repair fallback rebuilds groups from scratch and discards committed log, and Raft writes measure ~8–50× slower than `:strong`.

## What Changes

- Adopt `raft_ex` 0.5–0.7 behavior: pass through new throughput knobs (pipeline depth, replication batch size, pending-command bound, snapshot chunk size, compaction/segment options) with validated defaults; surface new observability (`server_metrics`/`cluster_metrics`, `replication_lag`, per-server counters) through existing stats/health; handle durable-write failures and bounded-queue backpressure as normal failures without raw exits.
- Harden Raft lifecycle: replace the log-discarding `form_cluster` fallback with safe in-place membership repair (join/promote/demote only, no silent data loss); bound leader-retry/reconcile loops with deadlines; normalize all new Raft error shapes to `SuperCache.Cluster.Error`.
- Harden distributed paths: audit sync/strong/raft write and primary/quorum read failover so timeouts, nodedown, and no-leader map to documented errors; bound health aggregation and handoff streaming; validate all new config at bootstrap.
- Improve performance: keep single-commit-per-partition-batch invariant while enabling Raft-side batching/pipelining; reduce hot-path overhead in storage (`ETS` count/projection, `select_delete`, `persistent_term` caching extended where safe), routing, and metrics; document measured latency/throughput targets and when `:raft` vs `:strong` should be chosen.

## Capabilities

### New Capabilities

- `raft-tuning`: tunable Raft throughput surface (pipeline, batching, compaction, snapshot-chunk knobs) with safe defaults, startup validation, and operator-visible effective config.

### Modified Capabilities

- `raft-replication`: engine-config coverage, durable-write/backpressure failure contract, observability via new metrics APIs.
- `raft-snapshot-recovery`: safe recovery without log discard; compaction and segment-storage behavior.
- `distributed-writes`: hardened failure propagation and batched-write performance in all modes including `:raft`.
- `storage-and-query-engine`: hot-path read/write efficiency without changing query semantics.
- `cluster-membership`: safe Raft member replacement and sizing diagnostics fed by new lag metrics.

## Impact

- Code: `lib/cluster/raft/{config,system,adapter,group,reconciler,machine}.ex`, `lib/cluster/{manager,replicator,router,health_monitor,stats}.ex`, `lib/storage/*`, `lib/api/*` write paths; `config/config.exs` docs; `benchmarks/distributed_benchmark.exs` and guides.
- Dependencies: no version bump needed (`raft_ex 0.7.0` already locked); removes reliance on removed `:seshat`/`:ra` env behavior; exposes new optional `config :super_cache, :raft` keys (backward compatible).
- Systems: Raft log/data-dir layout unchanged; no wire-protocol change; existing `:async`/`:sync`/`:strong` behavior preserved.
