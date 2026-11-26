# Design

## Context

See `proposal.md` for motivation. Current state that constrains the approach:

- Every upstream call is already funnelled through `SuperCache.Cluster.Raft.Adapter`, with `SuperCache.Test.RaftAdapterFake` providing deterministic responses in unit tests. `Raft.System` builds a named RaftEx system config from `Raft.Config`; `Raft.Group` and `Group.Reconciler` manage per-partition groups and membership; `Raft.Machine` applies committed commands into local ETS partitions; `Raft.Applied` holds an ETS watermark for idempotent replay.
- 0.4.0 keeps every upstream function this integration uses (`RaftEx.App.System.default_config/0`, `derive_names/1`, `start/1`, and the facade calls for clusters, servers, members, and commands). Its renamed predicate helpers are internal and have no call sites here. The renamed default-system startup path (`RaftEx.start/1`, `:raft_ex` app env) is not used because SuperCache starts its own named system directly.
- Newly usable facade surface: role-aware `add_member/4` plus `promote_member/3` / `demote_member/3`, `transfer_leadership/3`, `local_query/3` / `leader_query/3` / `consistent_query/3`, `key_metrics/2`, `member_overview/2`, and optional machine hooks (`handle_aux/5` or `/6`, `snapshot_installed/4`, snapshot effects).
- Local ETS partitions are created at bootstrap and are **not** persisted. A restart rebuilds them by replaying the committed Raft log. `Raft.Machine`'s state is only `%{partition_idx: idx}` — it does not contain record data.
- Reads are routed in `SuperCache.Cluster.Router` as generic ETS operations (`get`, `match`, `match_object`) with `:local`, `:primary`, and `:quorum` modes; read modes are not centrally validated.

## Goals / Non-Goals

**Goals:**

- Move to `raft_ex` 0.4.0 without changing existing write contracts or the public API.
- Keep the single-boundary invariant: no new direct upstream calls outside the adapter layer.
- Make leadership changes, membership roles, and read consistency use the guarantees the upgraded engine now provides.
- Preserve restart/ETS-recreation recovery exactly as specified today.

**Non-Goals:**

- Persisting full ETS partition contents inside Raft machine state or snapshots.
- Changing the partition-map algorithm, replication factors, or non-Raft replication modes.
- Removing `add_member/3` compatibility or renaming existing public functions.
- Reworking the generic read router into a Raft-specific data store.

## Decisions

### 1. Extend the adapter rather than calling the engine directly

Each new capability gets a thin wrapper in `Raft.Adapter` and a matching entry in the test fake; `system_module/0` keeps translating the `RaftEx` backend to `RaftEx.App.System`. Cluster code continues to depend only on the adapter.

*Why:* it preserves deterministic failure injection for membership, leadership-transfer, and query error branches, and keeps the experimental engine API behind one boundary. Calling the facade from `Router` or `Reconciler` directly would make those tests nondeterministic and scatter version-specific assumptions.

*Alternative rejected:* import the facade wherever needed — simpler to write but breaks the stated boundary invariant and the fake-based test strategy.

### 2. Query paths are consistency barriers, not the data source

`local_query`, `leader_query`, and `consistent_query` execute against **machine state**, which for SuperCache holds only the partition index. Reads therefore continue to fetch values from local ETS. The Raft query path is used to establish the required ordering/leadership guarantee before the ETS read:

- `:local` → local query barrier, then local ETS read.
- `:primary` → leader query barrier, then the existing primary routing (local read on the primary, `:erpc` otherwise).
- `:consistent` (new, opt-in) → quorum-confirmed query barrier, then a read on the leader's ETS partition.

A small query helper module supplies a trivial MFA that acknowledges the barrier; no business data crosses the Raft query boundary.

*Why:* this delivers the upgraded engine's leadership/confirmation guarantees without duplicating cache data into machine state or serializing every ETS record through Raft.

*Alternative rejected:* move partition records into machine state so queries return data directly — would change memory use, snapshot size, and every apply path, and would exceed this change's compatibility budget.

### 3. Explicit membership roles during reconciliation

`Group.member_ids/1` continues to return deterministic server ids. When the reconciler adds a member it calls the new four-argument `add_member` with `:promotable`, waits for the member's server to register and catch up, then promotes it with `promote_member/3`. On removal it calls `demote_member/3` before `remove_member/3`, guarded so the last voter is never demoted or removed. `start_cluster`/`start_or_restart_cluster` still create voters so initial formation and quorum-loss recovery retain today's behavior.

*Why:* adding a lagging node as a full voter can shrink the effective quorum; role-aware join/promote matches the engine's catch-up and last-voter safety checks.

*Alternative rejected:* keep joining all nodes as voters — no behavior change, but it fails to adopt a core 0.4.0 feature and leaves the existing catch-up gap.

### 4. Transfer leadership only on planned primary moves

A dedicated helper in the Raft layer takes a partition and its new primary: it reads the current leader, confirms the target is a member and caught up (via member info / metrics), then calls `transfer_leadership/3`. It is invoked from the partition-ownership change path **before** the old primary stops its group. Failure is logged, counted as a Raft failure, and falls through to today's form/reconcile + election path. Unplanned leader loss never triggers a transfer.

*Why:* the engine's `TimeoutNowRpc` path avoids a full election timeout during planned moves; retaining the old path as fallback keeps availability when the target is behind or unreachable.

*Alternative rejected:* always re-form the group on a primary move — correct but needlessly disruptive and loses the no-op-entry/quorum benefits.

### 5. Snapshots checkpoint the watermark; the log remains the data source

The machine implements the 0.4.0 snapshot hooks so the applied index is checkpointed and restored on snapshot installation. Because ETS partitions are empty after a restart and machine state contains no records, log entries required to rebuild a partition MUST be retained. Compaction is therefore not enabled for SuperCache groups in this change; snapshots carry metadata only. `snapshot_installed` distinguishes a normal restart (rebuild from remaining log, then resume from the watermark) from an ETS-recreation event (clear the watermark and replay everything).

*Why:* adopting the checkpoint API gives replay idempotency without silently dropping data that exists only in the log.

*Alternative rejected:* put a serialized partition image into machine state and compact freely — correct in the long term but changes memory, apply cost, and snapshot format well beyond this upgrade.

### 6. Observability reuses existing reports

`key_metrics/2` and `member_overview/2` data is folded into the existing `raft.groups` / group-detail structures in `Cluster.Stats` and health reports rather than introducing a new reporting surface. New counters reuse the Raft failure counter and add a small leadership-transfer failure count.

*Why:* operators already query these reports; adding fields is backward compatible.

## Risks / Trade-offs

- [Upstream API drift] 0.4.0 is still experimental and its facade may change again → keep all new calls behind the adapter and pin with `~> 0.4.0`, not `>= 0.4.0`.
- [Query barrier semantics] A quorum-confirmed barrier does not by itself guarantee the local ETS read is at the confirmed index → perform the `:consistent` read on the leader's partition and document that it is confirmed, not linearizable across all followers.
- [Leadership transfer races] A planned transfer can race a concurrent group stop or reconciliation → serialize transfers through the reconciler/ownership path and treat any `{:error, _}` or timeout as fallback, never as success.
- [Snapshot/replay interaction] Restoring a high watermark while ETS is empty could suppress needed replay → reset watermarks when tables are recreated and add a cluster test that restarts with a retained log.
- [Compatibility] Upstream renames and the `:ra` → `:raft_ex` env fallback could break a clean compile → run `mix clean && mix compile --warnings-as-errors` on the bumped dependency before layering features.
- [Working tree] The project has uncommitted Raft work and an archived harden-Raft change → build on the current tree and avoid reverting or overwriting those files.

## Migration Plan

1. Land the dependency bump and clean compile first; fix any upstream-renamed call sites before adding features.
2. Extend adapter + fake, then land role-aware membership and leadership transfer behind the existing reconciler.
3. Add query barriers and the opt-in consistent mode; keep existing read modes as the default.
4. Add snapshot watermark hooks with log retention, then enrich stats/health.
5. Run `mix test`, `mix test.cluster` (multi-node, alongside existing cluster tests), and `mix precommit`; update docs and the distributed skill in the same change.
6. Rollback is a revert of the change plus restoring `~> 0.3.1` in `mix.exs`/`mix.lock`; no persisted schema or on-disk format is introduced for cache data.
