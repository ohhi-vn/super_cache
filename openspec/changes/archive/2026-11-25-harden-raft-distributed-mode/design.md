# Design

## Context

See `proposal.md` for motivation. The current implementation constrains the approach:

- `SuperCache.Cluster.Manager` rebuilds `%{partition_idx => {primary, replicas}}` on every membership event, stores it in `:persistent_term`, and already spawns Raft reconciliation from the primary.
- `Raft.Group.member_ids/1` derives the group from `Manager.get_replicas/1`; `Manager` clamps the replica list to `min(replication_factor - 1, node_count - 1)`, so group size is already `min(replication_factor, node_count)`.
- `Raft.Group.Reconciler` serializes membership changes on the primary, using `members/2`, `add_member/3`, and `remove_member/3` when a leader exists and a forced `start_cluster/5` when it does not.
- `Raft.Machine` applies committed commands to ETS and relies on `Raft.Applied` to skip replayed indexes. `Applied.claim/2` currently advances the watermark *before* the apply.
- `Raft.submit/3` follows one leader redirect and retries `:no_leader` once when the local node is primary; it returns `{:ok, reply}` without checking whether the machine reply is an application error.
- `Manager.reconcile_ownership/2` and `sync_to_node/1` push ETS records directly to nodes that gain a partition. In raft mode this bypasses the Raft log.
- `raft_ex` snapshots persist only the machine metadata, not SuperCache's ETS contents, and the machine emits no release cursor, so a joining member currently receives historical log entries from index 1.
- `Stats.cluster().raft` and `HealthMonitor.local_node_health().raft` expose only `running` and a list of locally tracked partition indexes.

## Goals / Non-Goals

**Goals:**

- Make the `N + 1` recommendation observable without changing start/write behavior.
- Make leader loss and spare-member replacement routine, tested paths rather than emergent behavior.
- Make a joining member's state reconstruction correct for non-idempotent operations.
- Guarantee that a committed-but-unapplied command is never reported as success.
- Keep the existing `raft.groups` shape and add only additive observability fields.

**Non-Goals:**

- Hard rejection of under-provisioned clusters; the user chose warning-only behavior.
- Linearizable reads, read-index support, or changes to read modes.
- Changing `replication_factor`, partition hashing, or assignment semantics.
- Raft log compaction or snapshot-based ETS transfer. `raft_ex` snapshots do not carry ETS data; this change relies on the retained log and documents the limitation.
- Cross-partition atomic batches or changing non-Raft modes.
- Forking or patching `raft_ex`; every upstream call stays behind `Raft.Adapter`.

## Decisions

### 1. Topology diagnostic as derived state, evaluated by the membership owner

Add `SuperCache.Cluster.Raft.Topology` with a pure `evaluate(node_count, replication_factor)` returning:

```elixir
%{
  node_count: 2,
  replication_factor: 2,
  recommended_nodes: 3,
  status: :underprovisioned | :ok,
  evaluated_at: System.monotonic_time(:millisecond)
}
```

`Manager.update_partition_map/1` calls `Topology.evaluate/2` after publishing the map whenever `replication_mode == :raft`, stores the result in `:persistent_term`, and logs a warning only on a transition into `:underprovisioned` (and an info line on recovery). `Manager.init` evaluates the initial map as well, so the status is never stale from a previous run.

`Stats.cluster().raft` merges the stored topology under `:topology`; `HealthMonitor.local_node_health().raft` does the same and sets the node status to `:degraded` when the topology is under-provisioned (unless another check already reports `:unhealthy`).

Rationale: membership is already serialized in `Manager`, so this needs no new process and no bootstrap-time rejection. Rolling starts and single-node development keep working. An alternative — a hard check in `Bootstrap.start!/1` — was rejected because a fresh cluster's first node necessarily sees fewer than `N + 1` peers.

### 2. Keep the partition map as the single source of group membership

No new group-sizing configuration. The supported matrix falls out of the existing deterministic assignment:

| Cluster size vs. RF | Group size | Quorum | Intended use |
|---|---|---|---|
| 1 node | 1 | 1 | Development, embedded |
| `cluster < RF` | `cluster` | strict majority | Small clusters |
| `cluster == RF` | `RF` | strict majority | Minimum HA |
| `cluster == RF + 1` | `RF` | strict majority | Recommended: one spare for replacement |

Tests assert the derived sizes rather than introducing a separate code path. A spare is outside the voter set until a membership change assigns it.

### 3. Retry leader-placement failures, never ambiguous timeouts

`Raft.submit/3` gets an absolute deadline from `command_timeout_ms` (or the caller's `:timeout`) and a fixed attempt budget of three. It retries only errors that prove the command was not accepted: `:not_leader`, `:no_leader`, `:noproc`, and `:nodedown` observed before submission. Explicit `{:not_leader, leader}` replies re-target the named leader; bare `:not_leader`/`:no_leader` uses `find_leader/1` and, when this node is the primary, runs one reconciliation before retrying. A `:timeout` or post-submission exit is returned as a failure and is never resubmitted, because the entry may already be committed.

Rationale: correctness for non-idempotent commands requires distinguishing "not accepted" from "outcome unknown". Alternatives — retrying everything — can double-apply increments; retrying nothing — makes normal elections visible as write failures.

### 4. Rebuild a joining member from the log instead of handing it ETS data

In raft mode, `Manager.reconcile_ownership/2` and `Manager.sync_to_node/1` skip `push_partition`/`transfer_partition`; the ETS handoff contract in `cluster-membership` now applies only to non-Raft modes. A new `Raft.Group.prepare_member/2` runs on the target node and discards every piece of local state that could suppress replay: it force-deletes the local Raft server through a new `Adapter.force_delete_server/2` delegation (removing the server's persisted log and `last_applied`), clears the local ETS partition, and resets the applied-index watermark. Stopping the server alone is insufficient: `raft_ex` would recover `last_applied` from the retained data directory and skip the historical entries the member needs.

Preparation is triggered only for a member that is genuinely new relative to the group:

- When a leader exists, `Reconciler.join/5` prepares each member absent from the leader's current member set before starting and joining it.
- When no leader exists, `form_cluster/3` prepares only members that the previous partition map did not contain. `Manager` already has the old and new maps and passes the added-member set into reconciliation. If there is no previous map (cold boot of a fresh node), no preparation runs and normal Raft recovery is authoritative.
- A member that is still in the current member set when it restarts keeps its state and catches up incrementally. When a node loses group membership entirely, `Manager` stops its local group and clears its partition and watermark so a future rejoin starts clean.

The prepared member then starts, joins, and replays the committed log from the beginning; `Machine.apply/3` reconstructs puts, deletes, and increments exactly once.

Rationale: `raft_ex` snapshots carry machine metadata, not ETS contents, and a joining peer starts at `next_index: 1`. Seeding ETS through handoff and then replaying history double-applies increments and races with concurrent commits. Keeping a stale local log is equally wrong: recovery restores `last_applied` and the member never replays the entries whose effects were cleared. Rebuilding from the log with a discarded local server is the only path that preserves non-idempotent semantics. Alternative — recording the handoff snapshot's log index and starting replay after it — requires atomic coordination between the ETS scan and the Raft commit index that the current adapter cannot provide.

Trade-off: join time is proportional to the retained log length, and log compaction is deferred. The mode is opt-in, and the limitation is documented.

### 5. Advance the applied watermark only after a successful apply; surface apply results

Replace `Applied.claim/2` with a read/apply/record sequence: `Applied.applied?/2` for replay detection, then the command is applied, then `Applied.record/2` writes the index. Because a Raft server applies entries serially, the check-apply-record sequence does not need a lock. A failed apply returns `{:error, reason}` and leaves the watermark unchanged, so a later replay retries the command. A batch that fails partway may have applied its earlier operations; this is treated as an exceptional case, logged with the partition and index, and retried as a whole. Only indexes at or below a recorded watermark are acknowledged without re-applying.

`Raft.submit/3` normalizes machine replies: `:ok` and `{:ok, :already_applied}` become `{:ok, :ok}`; `{:error, reason}` becomes a normalized `SuperCache.Cluster.Error`; other replies are returned for forward compatibility. `Replicator` and `DistributedHelpers` already map `{:error, _}` to the documented write failure, so no public return shape changes.

Rationale: the previous behavior could report success for a committed-but-unapplied command and could permanently skip an index after a transient apply failure.

### 6. Additive group details in statistics and health

`Raft.Group.status/1` returns a bounded, rescue-protected snapshot:

```elixir
%{idx: 0, running: true, role: :leader | :follower | :unknown, leader: {name, node} | nil,
  member_count: 2, members: [{name, node}]}
```

`Group.all_status/0` maps it over `tracked/0`. `Stats.cluster().raft` keeps `running` and `groups` (a list of partition indexes) and adds `group_details` and `topology`. `HealthMonitor.local_node_health().raft` adds the same topology map and keeps the existing fields. Status reads use short timeouts (1 second) and never raise on an unreachable peer.

Rationale: preserving `groups` avoids breaking existing operators and tests; additive details make leadership and membership visible without another API.

### 7. Coverage strategy

- Unit tests exercise `Topology.evaluate/2`, transition logging, `submit` retry classification, machine-reply normalization, watermark ordering, `prepare_member` (including discarding a persisted server), skipped handoff, added-member detection from old/new maps, and group status.
- Cluster tests (tagged `:cluster`, run by `mix test.cluster`) cover: single-node raft; two nodes with RF 3; three nodes with RF 2 and a spare; leader kill with a surviving majority; spare replacement after a member stops; primary move after the old primary leaves; join after historical increments and deletes; rejoin with a stale local log; graceful leave; rolling restart; whole-cluster restart; network partition with lost quorum; and writes through tuple, batch, KeyValue, Queue, Stack, Struct, delete variants, and increments.
- Tests use unique peer names and per-test Raft data directories, following the existing `raft_cluster_test.exs` pattern.

## Risks / Trade-offs

- **[Unbounded log and slow joins]** A joining member replays the retained log; compaction is not implemented because snapshots lack ETS state → document the limitation, keep the mode opt-in, and gate log-size tuning through the existing `wal_max_size_bytes`; revisit log retention in a later change.
- **[Forced reformation after lost quorum]** `form_cluster/3` bypasses normal Raft reconfiguration and may start a fresh member alongside a survivor → prefer the surviving member's log, trigger election on the member with the longest log when possible, never prepare a survivor, restrict the path to "no leader", and test that committed data survives member replacement.
- **[Clearing a rejoin member's local state]** Local reads on that node may briefly return no data, and discarding the wrong server would lose committed data → only prepare members that are absent from the group's committed membership or from a known previous partition map; never prepare on a cold boot with no previous map; the node is not a serving replica for the partition at that point.
- **[Under-provisioned health noise]** Single-node and small clusters will report `:degraded` → keep the status additive, document that the warning describes sizing rather than availability, and never block startup or writes because of it.
- **[Apply failure after commit]** Surfacing an apply error does not undo the committed log entry → treat apply failures as exceptional, log the partition and index, and let replay converge on the remaining members; caller-level retry idempotency for new commands remains the caller's responsibility.
- **[Upstream API drift]** `raft_ex` is experimental → all calls stay in `Raft.Adapter`; the new behavior needs new delegations for `force_delete_server/2` alongside the existing membership and command functions.

## Migration Plan

No mandatory migration. Raft mode remains opt-in and off by default. Existing deployments keep their data directories and resume replay; the first membership event populates the new topology status. Rolling back to a non-Raft mode follows the existing procedure (stop cleanly, change `replication_mode`); Raft data directories can be retained in case the mode is re-enabled. The only visible change is additive health/stats data and the documented recommendation that a production cluster with `replication_factor: N` should run `N + 1` nodes.

## Open Questions

None. The `N + 1` semantics (warning, evaluated on membership change) were resolved with the user before planning; log compaction is an explicit non-goal rather than an unresolved decision.
