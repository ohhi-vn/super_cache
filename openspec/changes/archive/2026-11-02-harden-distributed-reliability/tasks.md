# Tasks

## 1. Baseline and prerequisites

- [x] 1.1 Run `mix deps.get` and `mix compile --warnings-as-errors`; verify all dependencies resolve and the project compiles cleanly before any edits.
- [x] 1.2 Run `mix test` on the unmodified code and record pass/fail counts, skips, and duration in `benchmarks/results/baseline.md`; verify the file exists and includes the exact commands used.
- [x] 1.3 Run `mix test.cluster` on the unmodified code and record pass/fail counts and duration alongside the unit baseline; verify any pre-existing failures are listed so later groups do not claim them as regressions.
- [x] 1.4 Run `mix run benchmarks/ets_benchmark.exs` and record local-mode medians and p95 values in `benchmarks/results/baseline.md`; verify the benchmark completes and the results are reproducible across two consecutive runs.
- [x] 1.5 Add `benchmarks/distributed_benchmark.exs` covering async, sync, and strong puts plus primary and quorum reads; run it against the unmodified code, record the baseline, and verify the script exits successfully with per-mode throughput and latency output.

## 2. Distributed error and RPC boundary

- [x] 2.1 Add `SuperCache.Cluster.Error` with `operation`, `partition`, `node`, `mode`, and `reason` fields plus a readable `message/1`; verify `test/cluster_error_test.exs` covers field population, message formatting, and `Exception.message/1`.
- [x] 2.2 Add `SuperCache.Cluster.Rpc` with `call/5`, `call!/5`, and `normalize/2` that converts node-down, timeout, remote exception, exit, and non-exception reasons into `%SuperCache.Cluster.Error{}`; verify `test/cluster_rpc_test.exs` covers each conversion and that `call!/5` raises the normalized exception.
- [x] 2.3 Update `SuperCache.safe/1` to catch `:exit` and `:throw` in addition to `rescue`, wrapping non-exception reasons in the documented error; verify `test/super_cache_test.exs` proves non-bang entry points return `{:error, %SuperCache.Cluster.Error{}}` instead of leaking an exit.
- [x] 2.4 Document the distributed error contract and its public failure semantics in `guides/Distributed.md` and the new module docs; verify `mix docs` completes and the guide section names the exception and the bang/non-bang behavior.

## 3. Bounded quorum primitive

- [x] 3.1 Add `SuperCache.Cluster.Quorum` with a deadline-bound `await/4` that accepts `{node, task_fun}` entries, counts `nil` as a valid value, rejects duplicate nodes, terminates early on quorum, and shuts down every remaining task on success or timeout; verify `test/quorum_test.exs` covers early success, nil majority, duplicate entries, timeout, and an empty caller mailbox after return.
- [x] 3.2 Register a dedicated `SuperCache.Cluster.Quorum.Pool` `Task.Supervisor` in `lib/application.ex`; verify the application starts cleanly and `test/quorum_test.exs` confirms a crashed quorum task cannot crash the caller.
- [x] 3.3 Replace `Replicator.await_quorum/3` with the bounded helper and remove the recursive `Task.yield_many/2` loop; verify the timeout test in `test/wal_test.exs` completes within the deadline and leaves no running tasks.
- [x] 3.4 Replace `Router.quorum_await/3` and `DistributedHelpers.await_quorum/7` with the bounded helper; verify `mix test test/distributed_helpers_test.exs test/cluster/router_ryw_test.exs` passes and quorum reads no longer poll or wait per task.
- [x] 3.5 Document quorum counting, early termination, and deadline behavior in `SuperCache.Cluster.Quorum` and `guides/Distributed.md`; verify the docs match the tested scenarios for nil results and duplicate acknowledgements.

## 4. Fast configuration and metrics

- [x] 4.1 Promote `:replication_mode`, `:replication_factor`, and `:started` to `SuperCache.Config` fast keys and make `Manager.replication_mode/0` and `Manager.replication_factor/0` read them without a GenServer call; verify `test/config_test.exs` suspends `SuperCache.Config` and still reads all three keys.
- [x] 4.2 Set `:started` to `false` during bootstrap initialization and `true` only after partitions and cluster state are ready, clearing it on stop; verify `test/cluster_bootstrap_lifecycle_test.exs` and `test/bootstrap_test.exs` cover failed and successful starts.
- [x] 4.3 Replace `Metrics.push_latency/2`'s cast-per-sample path with a fixed-size caller-written ETS ring buffer while preserving the public API; verify `test/metrics_test.exs` covers bounded retention, concurrent writers, and unchanged percentile reads.
- [x] 4.4 Add counters for async replication failures/drops, sync and strong quorum failures, WAL backpressure, read fallbacks, health timeouts, and handoff successes/failures; verify `test/metrics_test.exs` and `test/cluster_local_test.exs` confirm the counters increase and appear in `SuperCache.Cluster.Stats`.
- [x] 4.5 Document the fast keys, new counters, and latency-buffer behavior in `guides/Distributed.md` and the `SuperCache.Cluster.Metrics` moduledoc; verify `mix docs` builds and the documented names match the implementation.

## 5. WAL store and unified strong-write protocol

- [x] 5.1 Add `SuperCache.Cluster.WAL.Store` to own the WAL and acknowledgement ETS tables, start it before `SuperCache.Cluster.WAL` in `lib/application.ex`, and expose the table handles to the WAL process; verify `test/wal_test.exs` proves the tables and their contents survive killing and restarting the WAL GenServer.
- [x] 5.2 Refactor `SuperCache.Cluster.WAL` to use the store-owned tables without changing its existing public functions; verify the existing `test/wal_test.exs` suite passes and no table is created in the WAL GenServer's `init/1`.
- [x] 5.3 Implement `WAL.commit/2` as specified: validate partition and ops, append a pending entry with a monotonic sequence, apply locally, replicate to every replica, require a strict primary-inclusive majority, delete committed entries, and retain failed entries for recovery; verify `test/wal_test.exs` covers success, one-replica failure, all-replica failure, and duplicate acknowledgements.
- [x] 5.4 Add per-partition applied-sequence watermarks so recovery replay skips entries a node has already applied, and replay pending entries when the WAL process starts; verify `test/wal_test.exs` covers restart after apply-before-commit and proves put, delete, and increment replay are idempotent.
- [x] 5.5 Add WAL backpressure using a configurable pending-entry maximum and return `{:error, :wal_backpressure}` without appending when the cap is reached; verify `test/wal_test.exs` fills the queue and confirms the next write is rejected with no new entry.
- [x] 5.6 Route every `:strong` write through `WAL.commit/2` in `Router`, `DistributedHelpers`, `KeyValue`, `Queue`, `Stack`, and `Struct`, and stop selecting `ThreePhaseCommit` from the public write path; verify `mix test` plus a test that strong writes through each API leave the 3PC committed counter unchanged.
- [x] 5.7 Mark `SuperCache.Cluster.ThreePhaseCommit` as legacy in its moduledoc without removing its public functions; verify `mix compile --warnings-as-errors` succeeds and `mix test test/cluster/three_phase_commit_test.exs` still passes.
- [x] 5.8 Make strong batch writes produce one WAL commit per partition instead of one commit per record, aggregating per-partition failures; verify `test/distributed/batch_write_test.exs` covers a single-partition batch and a multi-partition batch with one failing partition.
- [x] 5.9 Implement the deterministic `{:increment, key, default, step}` WAL operation and use it from `KeyValue.increment/4` under `:strong`; verify `test/kv_test.exs` and `test/wal_test.exs` cover concurrent increments, replay, and removal of the read-modify-write race.
- [x] 5.10 Document strong-mode recovery, the applied-sequence watermark, backpressure, batch semantics, and the in-memory WAL boundary in `guides/Distributed.md` and `CHANGELOG.md`; verify the documented guarantees match the tests and the design's non-goals.

## 6. Write failure propagation through router and APIs

- [x] 6.1 Change `Router.local_write/3` and `Router.local_batch_write/2` to return `:ok` or `{:error, %SuperCache.Cluster.Error{}}`, and normalize forwarded-write failures in `route_write/4`; verify `test/cluster_local_test.exs` and `test/distributed/main_test.exs` cover local success, local quorum failure, and forwarded failure.
- [x] 6.2 Change `DistributedHelpers.apply_write/3` and `route_write/4` to propagate sync and strong failures instead of returning success, using the bounded quorum helper with a primary-inclusive majority; verify `test/distributed_helpers_test.exs` covers success with one replica down, total replica failure, and forwarded failure.
- [x] 6.3 Update the bang and non-bang write APIs in `SuperCache`, `KeyValue`, `Queue`, `Stack`, and `Struct` so bang variants raise the documented exception and non-bang variants return `{:error, exception}` after a replication failure; verify tests in `test/super_cache_test.exs`, `test/kv_test.exs`, `test/queue_test.exs`, `test/stack_test.exs`, and `test/struct_test.exs`.
- [x] 6.4 Verify public success return shapes are unchanged by running the existing distributed API tests (`test/distributed/*_test.exs`) with no assertion edits; record any unavoidable assertion change in the change notes and justify it against the compatibility requirement.
- [x] 6.5 Document the write failure contract and the behavior change from silent success to explicit failure in `guides/Distributed.md` and `CHANGELOG.md`; verify the guide's examples match the tested bang and non-bang results.

## 7. Async replication lifecycle

- [x] 7.1 Replace `Task.Supervisor.async_nolink/2` in `Replicator.do_replicate(:async, ...)` with a supervised `Task.Supervisor.start_child/2` worker that catches, logs, and counts all failures; verify a test performs many async writes and asserts the caller's message queue does not grow by one task-result message per write.
- [x] 7.2 Add bounded retry with backoff for transient replica failures, a configurable worker-pool limit, and a drop counter when the pool is full; verify `test/wal_test.exs` or a new `test/replicator_test.exs` covers retry success, retry exhaustion, and pool-full behavior without blocking the caller.
- [x] 7.3 Document async replication semantics, retry limits, and the failure/drop counters in `guides/Distributed.md` and the `SuperCache.Cluster.Replicator` moduledoc; verify the docs match the tested behavior.

## 8. Distributed read paths

- [x] 8.1 Update `Router.do_read(:primary)` to call the primary through `Rpc.call/5`, fall back to the local replica only when `has_partition?/1` is true, record a degraded-read counter, and raise the documented exception otherwise; verify `test/cluster_local_test.exs` and `test/distributed/main_test.exs` cover both fallback and no-replica failure.
- [x] 8.2 Apply the same primary-read fallback and failure contract to `DistributedHelpers.read_primary/4`; verify `test/distributed_helpers_test.exs` covers a reachable primary, an unreachable primary with a local replica, and an unreachable primary without one.
- [x] 8.3 Implement quorum reads through `Quorum.await/4`, counting the primary and every replica once, accepting `nil` as a value, terminating early on a strict majority, and falling back to the primary result with a degraded counter when no quorum forms; verify `test/distributed_helpers_test.exs` and `test/quorum_test.exs` cover nil majority, early success, timeout, and primary fallback.
- [x] 8.4 Update bang and non-bang read APIs so failed reads raise or return `{:error, exception}` instead of `[]`, including `SuperCache.get/2`, `get!/2`, `KeyValue.get/3`, `Queue.peek/2`, `Stack.peek/2`, and `Struct.get/3`; verify the corresponding unit and distributed tests cover each failure path.
- [x] 8.5 Preserve read-your-writes routing after successful writes and confirm the configured TTL still returns reads to local mode after expiry; verify `test/cluster/router_ryw_test.exs` passes without changing its expected behavior.
- [x] 8.6 Document read modes, failover rules, quorum fallback, and the read error contract in `guides/Distributed.md` and `CHANGELOG.md`; verify the guide distinguishes `:local`, `:primary`, and `:quorum` as tested.

## 9. Health monitor correctness

- [x] 9.1 Change `HealthMonitor.run_health_checks/1` to evaluate only the local node, and make `cluster_health/0` aggregate `local_node_health/0` from every live node through `Rpc.call/5` with a deadline; verify `test/cluster/health_monitor_test.exs` proves remote partition counts come from the remote node, not local ETS tables.
- [x] 9.2 Cache the last known health result for unreachable nodes and expose it as `:degraded` or `:unknown` rather than omitting the node; verify the cluster health test covers one unreachable peer and asserts the node still appears with a degraded status.
- [x] 9.3 Make replication-lag probes unique with `{:health_probe, node(), make_ref()}` and remove each probe in an `after` block on the primary and every replica; verify `test/health_monitor_deep_test.exs` covers concurrent probes and cleanup after a timeout.
- [x] 9.4 Bound each health cycle by a configured deadline, record timed-out checks as `:unhealthy` or `:unknown`, increment the health-timeout counter, and skip checks until `Config.get_config(:started, false)` is true; verify `test/cluster/health_monitor_test.exs` covers a slow peer, a pre-bootstrap cycle, and counter growth.
- [x] 9.5 Document node-local health checks, aggregation, probe cleanup, and timeout statuses in `guides/Distributed.md` and the `SuperCache.Cluster.HealthMonitor` moduledoc; verify `mix docs` builds and the status table matches the implementation.

## 10. Membership, handoff, and bootstrap validation

- [x] 10.1 Add a cache-ready handshake so only nodes that completed SuperCache bootstrap enter live membership, and have `Manager` and `NodeMonitor` ignore connected Erlang nodes that are not cache nodes; verify a cluster test connects a plain `:peer` without bootstrap and asserts it receives no primary or replica assignment.
- [x] 10.2 Ensure partition assignment is computed from the sorted live-node set so every node produces the same primary and replicas; verify `test/cluster/config_consistency_test.exs` covers joins and leaves in different orders and asserts identical maps.
- [x] 10.3 Rewrite `Manager.sync_to_node/1` and `reconcile_ownership/2` to select one deterministic donor per partition and coalesce duplicate in-flight transfers with an ETS transfer table; verify `test/cluster/cluster_test.exs` or a new `test/cluster/handoff_test.exs` asserts each partition is sent once when multiple holders exist.
- [x] 10.4 Rewrite `Replicator.push_partition/2` to stream bounded batches, retry transient batch failures with bounded backoff, keep successful batches, and return exact transferred and failed counts; verify a handoff test transfers more than one batch and asserts the transferred count equals the partition size and a failed batch reports its exact record count.
- [x] 10.5 Validate `replication_factor >= 1` and `num_partition > 0` in cluster bootstrap before any ETS table is created; verify `test/cluster_bootstrap_lifecycle_test.exs` asserts `ArgumentError` for invalid values and no table creation.
- [x] 10.6 Expose membership-change, handoff-success, handoff-failure, and transfer-failure counters in `SuperCache.Cluster.Stats`; verify `test/cluster_local_test.exs` and the handoff tests confirm the counters increase and are readable through `Stats.cluster/0` or `Stats.partitions/0`.
- [x] 10.7 Document cache-only membership, deterministic assignment, single-donor handoff, streaming transfer, and bootstrap validation in `guides/Distributed.md` and `CHANGELOG.md`; verify the guide matches the tested membership and handoff scenarios.

## 11. Integration verification

- [x] 11.1 Run `mix test` with the full unit suite and confirm every test passes with no new skips; verify the output matches the baseline except for new tests.
- [x] 11.2 Run `mix test.cluster` and confirm every cluster test passes, including node failure, handoff, health, and quorum scenarios; verify the run finishes within the configured test timeout.
- [x] 11.3 Run `mix run benchmarks/ets_benchmark.exs` and `mix run benchmarks/distributed_benchmark.exs` after the changes, compare medians and p95 against `benchmarks/results/baseline.md`, and record the comparison in `benchmarks/results/after.md`; verify no mode regresses beyond the agreed tolerance and every measured improvement is documented.
- [x] 11.4 Run `mix format --check-formatted`, `mix credo --strict`, and `mix dialyzer`; verify all three complete without errors and record the commands and results in the change notes.
- [x] 11.5 Run `mix docs` and review the generated documentation for the new modules and updated guide; verify no broken references or warnings are introduced.
- [x] 11.6 Audit the public API diff for changed function signatures, success return shapes, or mandatory config keys using `git diff` and the generated docs; verify none changed and record the audit result in the change notes.

## Workflow follow-up

- Archive the change after review and after the integration verification tasks pass.
- Deploy a strong-mode cluster as a coordinated upgrade; async and sync clusters may roll one node at a time.
- Track an optional disk-backed WAL as a separate follow-up change if VM-restart durability becomes a requirement.
