# Tasks

## 1. Dependency bump and compatibility baseline

- [x] 1.1 Change the `raft_ex` requirement in `mix.exs` from `~> 0.3.1` to `~> 0.4.0` and refresh `mix.lock` with `mix deps.update raft_ex`; verify `mix deps` reports 0.4.0 and the lockfile diff is limited to raft_ex and its transitive deps.
- [x] 1.2 Run `mix clean && mix compile --warnings-as-errors` and fix any upstream rename or behavior break; verify a clean compile with no warnings, before adding any new feature code.
- [x] 1.3 Run the existing Raft adapter and system unit tests plus `mix test --exclude cluster` and verify every pre-existing Raft test still passes on 0.4.0 with no test edits.

## 2. Adapter surface, deterministic fake, and configuration

- [x] 2.1 Add adapter wrappers and matching fake entries for `add_member/4` (membership role), `promote_member/3`, `demote_member/3`, `transfer_leadership/3`, `local_query/3`, `leader_query/3`, `consistent_query/3`, `key_metrics/2`, and `member_overview/2`; verify the fake's response installation and trace capture all new functions in adapter tests.
- [x] 2.2 Extend `Raft.Config` defaults, validation, and `Raft.System.system_config/0` with the 0.4.0 keys the integration uses (election timeout min/max, heartbeat, snapshot-related settings), keeping `data_dir`, WAL settings, and derived names intact; verify config unit tests cover valid values and rejection of invalid values.
- [x] 2.3 Add a CHANGELOG entry for the 0.4.0 upgrade and verify the version and behavior changes are described.

## 3. Role-aware membership

- [x] 3.1 Change reconciliation so a joining member is added with the `:promotable` role and promoted only after its server registers and catches up; verify reconciler unit tests assert the add-with-role then promote call order and the trace contains both calls.
- [x] 3.2 Change removal to demote before remove, with a guard that never demotes or removes the last voter; verify unit tests cover the happy path and the last-voter rejection scenario from the `cluster-membership` delta.
- [ ] 3.3 Run `mix test --exclude cluster` and a multi-node membership cluster test, and verify members converge to the configured voter set after a join and a leave.
- [x] 3.4 Update `guides/Distributed.md` membership section to describe promotable/voter roles and verify the documented behavior matches the reconciler calls.

## 4. Leadership transfer on planned primary moves

- [x] 4.1 Add a Raft-layer leadership-transfer helper that resolves the current leader, checks that the target is a caught-up voter, and calls the adapter's `transfer_leadership/3`; verify unit tests with the fake cover success, non-voter target, and not-caught-up target.
- [x] 4.2 Invoke the helper from the planned primary/ownership-change path before the old primary stops its group, and fall back to the existing form/reconcile + election path on error or timeout; verify a unit test asserts the fallback and that failures increment the Raft failure counter.
- [ ] 4.3 Add a cluster test for a planned primary move with a caught-up target and verify writes resume without an election-timeout stall; also verify unplanned leader loss still elects normally.
- [x] 4.4 Document planned-transfer behavior and its fallback in `guides/Distributed.md` and verify the text matches the implementation.

## 5. Raft-native query paths and consistent reads

- [x] 5.1 Add a query barrier helper that supplies a trivial MFA for local, leader, and quorum-confirmed queries, and wire `:local`, `:primary`, and new `:consistent` read modes through it in `SuperCache.Cluster.Router` while leaving the ETS read itself unchanged; verify router unit tests assert the barrier call for each mode and that result shapes are unchanged.
- [x] 5.2 Normalize barrier failures through the existing Raft error path and count them as Raft failures; verify tests cover quorum-unavailable consistent reads raising/returning the documented error rather than a stale value.
- [x] 5.3 Run `mix test --exclude cluster` and verify existing read-mode tests pass unchanged plus new tests for the opt-in consistent mode and its scoping to a single call.
- [x] 5.4 Document `:consistent` as an opt-in read mode in `README.md` and the distributed skill, and verify the documented examples use the new mode correctly.

## 6. Snapshot checkpointing and recovery

- [x] 6.1 Implement the 0.4.0 optional machine hooks so the applied index is checkpointed into snapshot metadata and restored on snapshot installation, without putting partition records into machine state; verify machine unit tests cover checkpoint advance, failed apply not advancing, and restore on install.
- [x] 6.2 Keep log entries needed for ETS rebuild available (do not enable compaction that discards them) and make `Raft.Applied` reset on ETS table recreation; verify applied-state and machine tests cover replay after restart and after table recreation.
- [ ] 6.3 Add a cluster restart test that stops a Raft group with a snapshot present, restarts with retained data dir, and verifies committed data is rebuilt exactly once; verify the test passes with `mix test.cluster`.
- [x] 6.4 Update the snapshots section of `guides/Distributed.md` to state that snapshots carry metadata while the log remains the data source, and verify the wording matches the implementation.

## 7. Observability integration

- [x] 7.1 Fold `key_metrics/2` and `member_overview/2` fields (term, commit/applied index, state, membership role) into the existing group-detail structures in `Cluster.Stats` and health reporting, keeping `raft.groups` unchanged; verify stats unit tests assert the new fields and backward compatibility.
- [x] 7.2 Add a leadership-transfer failure counter and reuse the Raft failure counter for query failures; verify tests assert both counters increase in the failure scenarios above.

## 8. Integration verification

- [ ] 8.1 Run `mix test`, `mix test.cluster`, and `mix precommit`; verify all gates pass with no warnings, formatting issues, Credo findings, or new Dialyzer errors.
- [x] 8.2 Run `openspec validate "upgrade-raft-ex-0-4" --strict` and verify every delta spec is valid, including the modified requirements' full content.

## Workflow follow-up

- Archive the change after the project's review requirements are satisfied.
- Verify the archived result with `openspec validate --strict` and a final `mix precommit`.
