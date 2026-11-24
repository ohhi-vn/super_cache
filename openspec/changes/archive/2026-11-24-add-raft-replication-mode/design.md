# Design

## Context

See `proposal.md` for motivation. Current state that shapes the approach:

- `SuperCache.Application` always starts Config, Manager, NodeMonitor, the replication/quorum worker pools, and the WAL/3PC infrastructure; `SuperCache.Cluster.Bootstrap.start!/1` validates options, writes Config, sets up NodeMonitor, verifies structural config with peers, then starts Partition, Storage, and Buffer, runs recovery, pulls a full sync, and marks `:started`.
- Partition ownership is computed by `Manager.get_replicas/1` as `{primary, replicas}` from the sorted live-node set. Replication dispatch lives in `SuperCache.Cluster.Replicator.replicate/3` and `replicate_batch/3`, which branch on `replication_mode` (`:async`, `:sync`, `:strong`). `:strong` goes through the in-memory `SuperCache.Cluster.WAL`.
- Config verification in Bootstrap already includes `:replication_mode` among structural keys, so adding a new mode value automatically participates in the "all nodes agree" check.
- `raft_ex 0.3.0` requires Elixir `~> 1.15` (compatible) and brings transitive `seshat ~> 0.6`. It exposes a system lifecycle (`start`/`start_in`/`start_cluster`), per-server `process_command`, a pluggable `RaftEx.Domain.Machine` behaviour, and member join/leave. Its README marks the project experimental: APIs unstable, consistent queries incomplete, and distributed snapshot transfer only partially tested.

## Goals / Non-Goals

**Goals:**

- Make Raft consensus usable as an opt-in `replication_mode: :raft` with no change to existing modes or public API success shapes.
- Keep every direct `raft_ex` call inside one adapter boundary so upstream API churn is contained.
- Reuse SuperCache's partition map, ETS storage, write paths, error type, metrics, and partition handoff rather than building a parallel data plane.
- Provide deterministic, partition-scoped Raft groups with recoverable, idempotent state-machine application.

**Non-Goals:**

- Replacing `:async`, `:sync`, or `:strong`, or changing their performance characteristics.
- Redefining SuperCache membership, partition hashing, or handoff to be "Raft-native"; Raft membership is derived from the existing partition map.
- New read guarantees beyond existing `:local`, `:primary`, and `:quorum` modes (leader-verified linearizable reads are upstream-incomplete and deferred).
- Hardening or forking `raft_ex` beyond the upstream compatibility fixes required to make multi-node work. Until those fixes are published, this project uses a local path dependency on `../raft_ex`; switch back to the pinned Hex release once a fixed version is published.

## Decisions

### 1. Adapter boundary around a pinned dependency

Create `lib/cluster/raft/` with a single public façade `SuperCache.Cluster.Raft` plus `Raft.System` (system lifecycle), `Raft.Group` (partition → group mapping and membership reconciliation), `Raft.Machine` (state machine behaviour implementation), `Raft.Applied` (ETS-backed applied-index store), and `Raft.Config` (defaults/validation). No other module may call `raft_ex` directly. Depend on `{:raft_ex, "~> 0.3.0", path: "../raft_ex"}` until the fixed build is published, then drop the `:path` option.

The local `../raft_ex` checkout carries upstream fixes verified by its 552-test suite: struct-safe `rpc_from` updates, WAL directory selection, `pid_to_server_id` on current OTP, applying `:apply_committed` effects, propagating commit indexes on heartbeats, handling heartbeats on followers, and suppressing duplicate client replies on followers.

**Rationale:** upstream APIs are explicitly unstable; a thin seam limits changes to one directory and lets the mode be disabled without loading Raft code paths. Alternative — calling `raft_ex` from `Replicator` directly — was rejected because it would scatter experimental API usage across the hot path and tests.

### 2. One Raft group per partition

For partition `idx`, derive a deterministic group name from the table prefix and `idx`. Its voting members are `{server_id, node}` pairs for the partition's primary and replicas from `Manager.get_replicas/1`; only nodes that own the partition host a server. `Raft.Group` owns creation, join/leave, and lookup.

**Rationale:** SuperCache's unit of ownership, quorum, batching, and handoff is already the partition. One group per partition preserves that model, avoids a single global leader bottleneck, and bounds work at `num_partition × replication_factor` servers. Alternative — one cluster for the whole cache — would serialize all partitions through one leader and force a redesign of batching and handoff.

### 3. State machine applies commands to the existing ETS partition

`Raft.Machine` implements the `RaftEx.Domain.Machine` behaviour. Its config carries the partition index. `apply/3` accepts the same operation tuples the WAL uses (`{:put, record|records}`, `{:delete, key}`, `{:delete_match, pattern}`, `{:delete_all, _}`, `{:increment, key, default, step}`) and applies them through the same local storage helpers used by `Replicator.apply_op/3`. The machine returns the operation result as the command reply.

Idempotency: `Raft.Applied` stores the highest committed log index applied per partition in an ETS table owned by a `Raft.Store` process (mirroring `WAL.Store`, so the table survives a machine-process restart). A command whose index is at or below the stored index is acknowledged without re-applying. This makes non-idempotent operations such as `increment` safe across log replay.

**Rationale:** reuse of the proven local-apply code plus an applied-index watermark gives at-most-once visible effects without changing ETS semantics. Alternative — relying on ETS writes being naturally idempotent — fails for counters.

### 4. Write path and leader routing

`Replicator.replicate/3` and `replicate_batch/3` gain a `:raft` clause: build one command (list of ops for batches), call `Raft.submit(partition_idx, command)`, which resolves the group and sends the command through the adapter. If the reply reports a different leader, retry once against that leader; if no leader resolves within `command_timeout_ms` (default `2_000`), fail. The caller blocks until commit, so a success value is only returned after majority commit and machine application.

Failures are mapped through `SuperCache.Cluster.Error` with operation, partition, and node/reason context, matching the existing bang/non-bang contract. `failure_field(:raft)` and the existing `replicate_<mode>` metrics make Raft writes visible without new metric plumbing.

**Rationale:** keeps the caller-visible contract identical to other modes; a single retry handles the common "stale leader" case without hiding a cluster that has no leader.

### 5. Lifecycle tied to Bootstrap and Manager

`Raft.System` is started lazily by `Bootstrap.start!/1` when `replication_mode == :raft`, after Partition/Storage/Buffer are up and membership is known, and is stopped before `Storage.stop/0`. The system data directory comes from `config :super_cache, :raft` (default derived from the app dir, overridable per environment; tests use a temp dir). On bootstrap, groups are created for partitions owned by this node. `Manager` notifies `Raft.Group` on node-up/node-down so members are added/removed deterministically; a new owner is first seeded through the existing partition handoff, then joined as a member. On restart, `Raft.System` starts from the persisted log and `Raft.Applied` prevents duplicate effects.

**Rationale:** Raft needs both storage (for the machine) and membership (for the member set) before it can start; Bootstrap is the only place that sequence is guaranteed. Alternative — starting Raft as an unconditional application child — would create data dirs and servers for local-only users.

### 6. Configuration and validation

Add `:raft` to the accepted mode lists in `Cluster.Bootstrap`, `Config`, and related guards; add an optional `config :super_cache, :raft` block exposing `data_dir`, `command_timeout_ms`, `wal_sync_method`, and `wal_max_size_bytes`. No new mandatory key. Document the mode in README, `guides/Distributed.md`, the config table, and `skills/super-cache-distributed/`.

**Rationale:** existing config verification already compares `:replication_mode` across nodes, so mixed-mode clusters are rejected before any Raft group starts.

### 7. Tests

Unit tests cover config validation, group naming/member derivation, machine apply and applied-index replay, and error mapping. Cluster tests (`:peer`-based, run under the existing `test.cluster` alias or a new `test.raft` alias) cover leader election, committed writes through multiple APIs, leader redirect, lost-quorum failure, restart recovery, and membership change. Tests isolate Raft data in temp directories and never require a different Mix env.

## Risks / Trade-offs

- **[Upstream instability]** `raft_ex` is experimental with changing APIs → pin `~> 0.3.0`, confine usage to the adapter, keep the mode opt-in, and avoid depending on incomplete features (leader-verified reads, full snapshot transfer).
- **[Performance]** persistent WAL + fsync is slower than the in-memory WAL used by `:strong` → keep it opt-in, expose `wal_sync_method`, and document that `:raft` trades latency for a real consensus log.
- **[Membership skew]** concurrent SuperCache membership events and Raft reconfiguration could diverge → serialize reconfiguration through `Raft.Group`, derive membership only from `Manager`, and test join/leave convergence.
- **[Replay correctness]** replaying non-idempotent ops could double-apply → `Raft.Applied` index watermark with tests for restart replay.
- **[State transfer]** upstream snapshot transfer is only partially tested → seed new owners through existing SuperCache partition handoff and treat Raft snapshots as log compaction only.
- **[Licensing]** MPL-2.0/Apache-2.0 dependency in an MIT project → verify redistribution obligations before a release that ships this dependency; no changes to upstream source are planned.
- **[Dependency chain]** transitive `seshat ~> 0.6` adds a runtime dep → accept it as part of the pinned `raft_ex` version and confirm it loads on all supported OTP versions during implementation.

## Migration Plan

No migration is required: the mode is opt-in and defaults are unchanged. Rollout is documentation + an optional dependency. Enabling `:raft` on a cluster requires all nodes to start with the same `replication_mode: :raft` and Raft data dirs to be writable; the existing config verification rejects mixed-mode starts. Rollback is setting `replication_mode` back to a previous mode after a clean stop; operators should back up Raft data dirs if they may later re-enable the mode.

## Open Questions

None that change the specs, approach, or task breakdown. Deferred to implementation verification only: the exact `raft_ex` 0.3.0 function signatures for leader redirect and membership join/leave, and whether its `seshat` dependency compiles on every supported OTP version.
