# Tasks

## 1. Topology diagnostics (N+1)

- [x] 1.1 Add `SuperCache.Cluster.Raft.Topology` with `evaluate/2`, `refresh/1`, and `status/0`, storing the latest result in `:persistent_term` and logging only on status transitions; verify unit tests cover 2 nodes with RF 2 (under-provisioned), 3 nodes with RF 2 (healthy), a single node with RF 1 or RF 2, and RF greater than the node count, including the warning transition.
- [x] 1.2 Evaluate topology from `Manager` after the initial partition map and after every membership update when `replication_mode == :raft`; verify a unit test asserts status changes after `node_up`/`node_down` and that non-raft modes perform no evaluation.
- [x] 1.3 Merge `topology` into `Stats.cluster().raft` and `HealthMonitor.local_node_health().raft`, and set health to `:degraded` when under-provisioned while keeping existing `running` and `groups` fields unchanged; verify tests assert both statuses and the preserved field shapes.
- [x] 1.4 Document the N+1 recommendation, warning-only behavior, and additive status fields in `README.md`, `guides/Distributed.md`, and `skills/super-cache-distributed/SKILL.md`; verify the documented values match the implemented field names and that single-node validity is stated.

## 2. Submission safety, leader failover, and apply semantics

- [x] 2.1 Replace `Applied.claim/2` with an apply-then-record sequence used by `Raft.Machine`; verify unit tests show the watermark does not advance when an apply fails and that a later retry applies the command instead of skipping it.
- [x] 2.2 Normalize machine replies in `Raft.submit/3`: `:ok` and `{:ok, :already_applied}` are success, `{:error, reason}` becomes a `SuperCache.Cluster.Error`; verify tests cover each reply shape and that `Replicator`/`DistributedHelpers` still return the documented success shapes.
- [x] 2.3 Add bounded leader retry within the command deadline for `:not_leader`, `:no_leader`, `:noproc`, and pre-submission `:nodedown`, with at most three attempts and no resubmission after an ambiguous `:timeout`; verify adapter-coverage tests cover redirect, bare not-leader discovery, local-primary reconciliation, deadline exhaustion, and the no-retry-on-timeout case.
- [x] 2.4 Document the failover, redirect, retry, and ambiguous-timeout contracts in `guides/Distributed.md` and the distributed skill; verify the documented error behavior matches the tests from 2.2 and 2.3.

## 3. Join, catch-up, and spare replacement

- [x] 3.1 Add `Adapter.force_delete_server/2` delegation and `Raft.Group.prepare_member/2` that force-deletes the local Raft server (persisted log and `last_applied`), clears the local ETS partition, and resets the applied-index watermark; verify unit tests assert the order, that a stale local server is actually discarded, and that repeating preparation is safe.
- [x] 3.2 Trigger preparation only for genuinely new members: from `Raft.Group.Reconciler` for members absent from the leader's current member set, and from the no-leader `form_cluster` path for members absent from the previous partition map (passed from `Manager`); verify tests cover a new member, a member already in the set, a cold boot with no previous map (no preparation), and a failed preparation.
- [x] 3.3 Skip ETS partition handoff and full-sync pushes for Raft-managed partitions in `Manager.reconcile_ownership/2` and `Manager.sync_to_node/1`, and clear the local partition/watermark when a node loses membership entirely; verify unit tests assert no `push_partition` in raft mode, unchanged handoff in other modes, cleanup on membership loss, and added-member detection from old/new maps.
- [ ] 3.4 Add a three-node `replication_factor: 2` cluster test that writes increments, stops a voting member, and verifies the spare is joined, previously committed data survives, and every member reports the same counter; add a rejoin case with a stale local Raft log and verify the member replays history instead of skipping it.
- [x] 3.5 Update `cluster-membership` and Raft documentation to describe log-based member rebuild, discarding stale local Raft state, the no-handoff rule, and the full-log catch-up limitation; verify the docs match the scenarios in the spec delta.

## 4. Group observability

- [x] 4.1 Add `Raft.Group.status/1` and `all_status/0` returning partition, running state, role, leader, member count, and members with a bounded timeout and rescue handling; verify unit tests cover a leader, a follower, and an unreachable group.
- [x] 4.2 Expose `group_details` in `Stats.cluster().raft` and the local health `raft` map while keeping `raft.groups` as the existing list of partition indexes; verify stats and health tests assert the additive fields and the preserved `groups` shape.
- [x] 4.3 Document the new `group_details` and topology fields in `README.md`, `guides/Distributed.md`, and the distributed skill; verify the field names match the implementation.

## 5. Topology and recovery coverage

- [ ] 5.1 Add cluster tests for the supported topology matrix: single-node raft, two nodes with `replication_factor: 3`, three nodes with `replication_factor: 2`, and a cluster exactly at the replication factor; verify each test asserts the derived group size and a committed write.
- [ ] 5.2 Add cluster tests for leader kill with a surviving majority, lost quorum, graceful leave, primary move after the old primary leaves, rolling restart, and whole-cluster restart with persisted Raft directories; verify each test asserts recovery, converged data, and resumed writes.
- [ ] 5.3 Add cluster tests for every write surface under raft: tuple put/delete, batch within one partition, cross-partition batch, KeyValue, Queue, Stack, Struct, `delete_match`, `delete_all`, and `increment`; verify each test asserts the final ETS state on all group members.
- [x] 5.4 Update `CHANGELOG.md`, `README.md`, and `guides/Distributed.md` with the supported matrix, N+1 recommendation, failover behavior, and log-catch-up limitation; verify the documented behavior matches the implemented tests.

## 6. Integration verification

- [x] 6.1 Run `mix format --check-formatted`, `mix credo --strict`, and `mix dialyzer`; verify all pass with no new warnings.
- [x] 6.2 Run `mix test --exclude cluster`; verify the full unit suite passes with no leaked Raft processes or temp directories.
- [x] 6.3 Run `mix test.cluster`; verify the full cluster suite, including the new topology, failover, replacement, restart, and write-surface tests, passes.
- [x] 6.4 Run `openspec validate "harden-raft-distributed-mode" --strict`; verify the change validates cleanly.

## Workflow follow-up

- Archive the change after the project's review requirements are satisfied.
- Verify the archived result and the updated main specs.
