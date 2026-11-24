# Tasks

## 1. Dependency and API verification

- [x] 1.1 Add `{:raft_ex, "~> 0.3.0"}` to `mix.exs` deps (currently with `path: "../raft_ex"` until the upstream fix is published), run `mix deps.get` to update `mix.lock`, and verify `mix deps` lists `raft_ex` and transitive `seshat` with a clean `mix compile`.
- [x] 1.2 Verify the exact `raft_ex 0.3.0` API surface (system start/stop, cluster start, `process_command` leader-redirect reply, `RaftEx.Domain.Machine` callbacks, member join/leave) with a throwaway script; verify single- and multi-node groups commit, applying the required fixes in the local `../raft_ex` checkout.

## 2. Configuration surface

- [x] 2.1 Add `:raft` to every replication-mode validation list (`Cluster.Bootstrap`, `Config`, related guards) and verify unit tests accept `:raft` while still rejecting unknown modes.
- [x] 2.2 Add `SuperCache.Cluster.Raft.Config` with defaults for `data_dir`, `command_timeout_ms` (2_000), `wal_sync_method`, and `wal_max_size_bytes`, and verify tests cover default resolution and invalid values.

## 3. Raft adapter core

- [x] 3.1 Implement `Raft.Store` and `Raft.Applied` (ETS applied-index per partition) and verify a unit test shows replaying a lower/equal index does not re-apply an `increment`.
- [x] 3.2 Implement `Raft.Machine` applying `put`/`delete`/`delete_match`/`delete_all`/`increment` to the target partition through the same local storage helpers as `Replicator.apply_op/3`, and verify unit tests for each op and idempotent replay.
- [x] 3.3 Implement `Raft.System` lifecycle (start with configured data dir, start/stop groups, recover persisted log) and verify a unit test starts and stops it with a temp data dir and no leaked processes.
- [x] 3.4 Implement `Raft.Group` with deterministic group naming and member derivation from `Manager.get_replicas/1`, plus serialized join/leave reconciliation, and verify unit tests show identical membership on nodes with the same partition map and convergence after a member leaves.
- [x] 3.5 Implement the public `SuperCache.Cluster.Raft.submit/1,2` façade with one leader-redirect retry and error normalization to `SuperCache.Cluster.Error`, and verify unit tests cover redirect, timeout, and error context.

## 4. Write-path integration

- [x] 4.1 Add the `:raft` clause to `Replicator.replicate/3` and `replicate_batch/3`, including `replicate_raft` metrics and a `raft_failures` failure field, and verify unit tests show dispatch to the adapter and counter increments.
- [x] 4.2 Confirm tuple, KeyValue, Queue, Stack, Struct, and batch writes all use the same Raft submission path, and verify a cluster test shows equivalent writes through two different APIs commit identically.
- [x] 4.3 Wire `Bootstrap.start!/1`/`stop/0` to start `Raft.System` after Partition/Storage/Buffer and stop it before Storage teardown, and have `Manager` membership changes call `Raft.Group` reconciliation, and verify a bootstrap test asserts the lifecycle order.

## 5. Observability and documentation

- [x] 5.1 Expose Raft group status and write/failure counters through `Cluster.Stats`/`Metrics` and health reporting, and verify a stats test shows the new values after a failed Raft write.
- [x] 5.2 Document `:raft` mode, config keys, experimental-upstream caveat, and license note in README, `guides/Distributed.md`, the config reference table, `CHANGELOG.md`, and `skills/super-cache-distributed/`, and verify the documented configuration starts a node in raft mode.

## 6. Integration verification

- [x] 6.1 Add cluster tests for leader election, committed writes, follower redirect, lost-quorum failure, restart recovery, and membership change, and verify `mix test.cluster` passes them.
- [x] 6.2 Run the full quality gates (`mix format --check-formatted`, `mix credo --strict`, `mix dialyzer`, `mix test --exclude cluster`, `mix test.cluster`) and verify all pass with no new warnings.

## Workflow follow-up

- Archive the change after the project's review requirements are satisfied.
- Verify the archived result.
