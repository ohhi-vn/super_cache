# Proposal

## Why

`raft_ex` 0.4.0 is published on Hex and turns several previously experimental paths into working behavior — multi-server elections, consistent queries, graceful leadership transfer, role-aware membership, and snapshot checkpointing. SuperCache is still pinned to `~> 0.3.1` and its Raft boundary deliberately hides the upstream API, so the new capabilities are currently unavailable and the integration has not been checked against the 0.4.0 behavior changes. Upgrading now keeps the opt-in `:raft` mode on a supported API surface and lets planned primary moves and reads use Raft-native guarantees instead of retries and local reads.

## What Changes

- **BREAKING** (upstream): bump `{:raft_ex, "~> 0.3.1"}` to `{:raft_ex, "~> 0.4.0"}` in `mix.exs` and refresh `mix.lock`. Upstream 0.4.0 renames predicate helpers (`is_dir/1` → `dir?/1`, `is_file/1` → `file?/1`, `is_any_file/1` → `any_file?/1`, `is_versioned/1` → `versioned?/1`, `is_new?/1` → `new?/1`, `is_fully_persisted?/1` → `fully_persisted?/1`) and switches default-system startup to the `:raft_ex` application environment with `:ra` kept only as a one-release fallback. SuperCache calls only the facade and `RaftEx.App.System` functions, so no helper call sites are expected to break; this must be verified by a clean compile.
- Extend the single Raft boundary (`SuperCache.Cluster.Raft.Adapter` and the test fake) with the 0.4.0 APIs SuperCache will use: `add_member/4` with an explicit membership role, `promote_member/3`, `demote_member/3`, `transfer_leadership/3`, `local_query/3`, `leader_query/3`, `consistent_query/3`, `key_metrics/2`, and `member_overview/2`. Verify every existing call site is still valid under 0.4.0.
- Reconcile partition groups with explicit Raft roles. New members join as `:promotable` and are promoted once caught up; removed primaries are demoted before removal, with last-voter safety preserved.
- On planned primary/ownership moves, ask the current leader to transfer leadership to the new primary (via `TimeoutNowRpc`) before stopping the old server, so commits resume without waiting for an election timeout.
- Route reads of Raft-managed partitions through Raft query paths: local queries for `:local`, leader queries for `:primary`, and quorum-confirmed consistent queries for an explicit consistent read mode. Existing read modes and success shapes remain unchanged unless a caller opts in to the new mode.
- Integrate 0.4.0 snapshot checkpointing with `SuperCache.Cluster.Raft.Applied`: emit release-cursor/checkpoint effects at applied indexes, install/recover watermark state from snapshots on restart, and keep replay idempotent when ETS partitions are recreated.
- Enrich Raft observability with upstream `key_metrics/2` and `member_overview/2` data (term, commit index, applied index, membership, state) in existing stats and health reports; expose leadership-transfer and query failures through the existing Raft failure counters.
- Update tests, the deterministic adapter fake, `README.md`, `guides/Distributed.md`, `CHANGELOG.md`, and the distributed-mode skill to describe the new behavior and the 0.4.0 version.

## Capabilities

### New Capabilities

- `raft-query-paths`: Raft-native local, leader, and consistent query paths for Raft-managed partitions, including the opt-in consistent read mode and its failure contract.
- `raft-leadership-transfer`: Graceful leadership handoff during planned primary moves, including fallback when the transfer cannot complete.
- `raft-snapshot-recovery`: Snapshot checkpointing, installation, and recovery of the applied-index watermark so replay stays idempotent across restarts and ETS recreation.

### Modified Capabilities

- `raft-replication`: 0.4.0 API/config compatibility, Raft-native read-path interaction, role-aware group membership, and expanded observability requirements.
- `cluster-membership`: Voter/promotable/non-voter roles for Raft partition groups when cache nodes join, are promoted, or leave.

## Impact

- Dependencies: `mix.exs`, `mix.lock` (`raft_ex` 0.3.1 → 0.4.0).
- Code: `lib/cluster/raft.ex` and `lib/cluster/raft/{adapter,system,config,group,applied,machine,stats}.ex`, `lib/cluster/raft/group/reconciler.ex`, plus any membership hook that changes partition ownership.
- Tests: `test/support/raft_adapter_fake.ex`, adapter/group/reconciler/machine unit tests, and `test/cluster/raft_cluster_test.exs` cluster integration tests.
- Docs and specs: `README.md`, `guides/Distributed.md`, `CHANGELOG.md`, `skills/super-cache-distributed/SKILL.md`, and the five capability specs listed above.
- Compatibility: existing `:async`, `:sync`, `:strong`, and `:raft` write contracts must remain unchanged; no public function is removed.
