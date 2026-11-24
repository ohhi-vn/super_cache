# Proposal

## Why

SuperCache's distributed mode currently implements its own replication protocols (`:async` fire-and-forget, `:sync` adaptive quorum, and `:strong` WAL-based consistency). These are hand-rolled, in-memory, and tied to SuperCache's primary/replica partition map; they do not provide a real consensus log with leader election, persistent membership, or snapshots. `raft_ex` (Elixir port of RabbitMQ RA, published on Hex as `0.3.0`) provides those primitives. Adding it as an **opt-in** replication mode lets users choose genuine Raft consensus for workloads that need it, without changing or destabilizing the existing modes. `raft_ex` is explicitly experimental with unstable APIs, so it must be isolated behind a thin adapter and kept strictly opt-in.

## What Changes

- Add `{:raft_ex, "~> 0.3.0"}` to `mix.exs` dependencies and lock it in `mix.lock`.
- Add `:raft` as a new accepted value of `replication_mode` in Bootstrap validation, `Config`, cluster config verification/export, and documentation. `:async`, `:sync`, and `:strong` remain the defaults and behave exactly as before.
- Introduce a `SuperCache.Cluster.Raft` adapter module that owns the `raft_ex` system lifecycle and maps SuperCache partitions to Raft server groups (one Raft cluster per partition, voting members drawn from the partition's primary/replicas).
- Implement a `RaftEx.Domain.Machine` state machine that applies committed cache operations (`put`, `delete`, `delete_match`, `delete_all`, `increment`) to the local ETS partition, so the replicated log is the source of truth in `:raft` mode.
- Route `:raft` writes through the Raft log (single commit per operation/batch), follow leader redirects, and normalize Raft failures to `SuperCache.Cluster.Error`. No write may report success before its log entry is committed.
- Start/stop and recover Raft servers alongside partition bootstrap/teardown; expose Raft status and counters through existing cluster stats and health monitoring.
- Add configuration for Raft data directory and tuning (e.g. WAL sync method, timeouts) under `config :super_cache, :raft`.
- Document the mode in README, `guides/Distributed.md`, config reference, and the distributed AI-agent skill; note the experimental upstream status.

**Breaking changes:** None. `:raft` is opt-in; all existing modes, defaults, config keys, and public API return shapes are unchanged.

## Capabilities

### New Capabilities

- `raft-replication`: Opt-in Raft-consensus replication — mode activation and config validation, per-partition Raft server groups and state-machine application to ETS, leader-based committed writes and failure propagation, bootstrap/recovery lifecycle, and observability.

### Modified Capabilities

(none — existing `distributed-consistency`, `distributed-writes`, and `cluster-membership` requirements describe the current modes and keep their behavior; the new mode is additive.)

## Impact

- **Code:** `mix.exs`, `SuperCache.Cluster.Bootstrap`, `SuperCache.Config`, `SuperCache.Application` (supervision of the Raft system), `SuperCache.Cluster.Replicator` (mode dispatch), `SuperCache.Cluster.Manager` (membership → Raft server config), `SuperCache.Cluster.Stats`/`Metrics`, plus new modules under `lib/cluster/raft/`.
- **APIs/config:** New optional `replication_mode: :raft` value and new optional `config :super_cache, :raft` keys. No mandatory config and no change to existing return shapes.
- **Dependencies:** New runtime dependency `raft_ex ~> 0.3.0` (Elixir `~> 1.15`, matching this project) with transitive dependency `seshat ~> 0.6`. It is dual-licensed MPL-2.0 / Apache-2.0 — confirm redistribution obligations before publishing, since SuperCache is MIT. It requires distributed Erlang for peer RPC, consistent with SuperCache distributed mode.
- **Tests/docs:** New unit tests for the adapter/machine and cluster tests for leader election, committed writes, leader failover, and recovery; updates to README, distributed guide, config table, and `skills/super-cache-distributed/`.
- **Risk:** `raft_ex` is pre-1.0/experimental with unstable APIs and incomplete consistent-query support. The adapter boundary and version pin contain that risk; leader-verified reads beyond existing `:primary`/`:quorum` modes are out of scope for this change.
