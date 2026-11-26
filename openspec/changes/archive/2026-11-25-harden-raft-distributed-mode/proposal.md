# Proposal

## Why

The opt-in `:raft` replication mode introduced in 1.7.0 covers the happy path, but a distributed cache is only as useful as its behavior during membership change, leader loss, restart, and under-provisioned clusters. Several common topologies and failure paths are currently untested, and some are quietly incorrect — most notably, a newly added Raft member can replay historical log entries on top of data seeded through partition handoff, double-applying non-idempotent operations such as `increment`.

Operators also lack a clear signal that a cluster with `replication_factor: N` has fewer than `N + 1` nodes, which is the sizing needed to keep a spare node available for failover and repair. Raft mode should warn and expose that condition, but must not refuse to start or write: small and single-node clusters remain valid.

## What Changes

- Add a Raft topology diagnostic evaluated whenever cluster membership changes: when live nodes are fewer than `replication_factor + 1`, log a warning and report `underprovisioned` through cluster stats and health. Startup and writes continue; the status clears automatically when enough nodes join.
- Define and support the common Raft topology matrix: single node, cluster size below the replication factor (group clamped to available nodes), cluster size equal to the replication factor, and `N + 1` nodes with a spare for replacement. Document the recommended sizing.
- Make leader failover a first-class path: when a group loses its leader and still has a majority, elect a new leader without operator intervention; retry only leader-placement failures (not ambiguous command timeouts) within the command deadline.
- Make spare-node replacement work after a member is lost: recompute the group from the partition map, join the spare, and resume commits once a majority exists.
- Fix member catch-up: a joining member rebuilds its partition from the committed Raft log instead of accepting ETS handoff for the same partition, so replay cannot double-apply increments. Discard the member's stale local Raft server and applied-index watermark when its state is rebuilt, and do not advance the watermark before a command applies successfully.
- Propagate state-machine apply failures instead of reporting a committed-but-unapplied write as success.
- Cover restart and recovery for both a single node and a whole cluster, including recovery of committed data after every node restarts.
- Expand Raft observability with topology status and per-group leader/member/role details while preserving the existing `raft.groups` list shape.
- Add unit and cluster coverage for the matrix above, including every write surface (tuple, batch, KeyValue, Queue, Stack, Struct, delete variants, and increments).

**Breaking changes:** None. `:raft` remains opt-in; existing modes, defaults, public return shapes, and existing stats keys are preserved. New diagnostics are additive.

## Capabilities

### New Capabilities

(none — the behavior belongs to the existing Raft and membership capabilities.)

### Modified Capabilities

- `raft-replication`: under-provisioned topology diagnostics, supported topology matrix, leader failover and safe retry, spare-member replacement and catch-up, apply-result propagation, watermark safety, whole-cluster recovery, and richer group observability.
- `cluster-membership`: scope ETS partition handoff to non-Raft modes and define the Raft-mode catch-up path, where a new member's partition state comes from the committed log rather than a direct ETS copy.

## Impact

- **Code:** `SuperCache.Cluster.Manager` (membership-change evaluation and no-handoff path), `SuperCache.Cluster.Raft` façade (retry/reply handling), `SuperCache.Cluster.Raft.Group` and `Group.Reconciler` (join/replace/prepare member), `Raft.Machine` and `Raft.Applied` (watermark sequencing), new `Raft.Topology` diagnostic, `Cluster.Stats`, `Cluster.HealthMonitor`, and `Cluster.Replicator`/`DistributedHelpers` reply handling.
- **APIs/config:** No new mandatory configuration. `cluster_stats().raft` and local health gain additive topology/group fields; `raft.groups` keeps its current shape.
- **Tests/docs:** New unit tests and `:peer`-based cluster tests for the topology matrix, leader kill, spare replacement, join/leave, rolling restart, full-cluster restart, partition/lost-quorum behavior, and all write surfaces. Update README, `guides/Distributed.md`, `CHANGELOG.md`, and `skills/super-cache-distributed/SKILL.md`.
- **Risk:** Raft catch-up currently relies on replaying the full retained log; log compaction/snapshot-based state transfer remains an upstream limitation and is explicitly documented rather than relied on.
