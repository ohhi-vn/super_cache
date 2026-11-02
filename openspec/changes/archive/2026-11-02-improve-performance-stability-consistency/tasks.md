# Tasks

## 1. Storage Engine, MatchSpec, and Supervised RYW Table

- [x] 1.1 Implement recursive, numerically ordered capture-variable extraction (`MatchSpec.extract_bindings/1`) in `lib/storage/match_spec.ex` and wire `Storage.get_by_match/2` in `lib/storage/storage_api.ex` to use it; verify `mix test test/match_spec_test.exs test/storage_api_test.exs` passes with new tests covering nested tuple patterns (`{{:ns, :"$1"}, :"$2"}`) and out-of-order variables (`{:rec, :"$2", :"$1"}`).
- [x] 1.2 Move creation and ownership of `SuperCache.Cluster.Router.RywTracker` to `SuperCache.Cluster.WAL.Store` in `lib/cluster/wal/store.ex` and update `Router.ensure_ryw_table/0` and `Router.prune_ryw/1` in `lib/cluster/router.ex` to use `:ets.select_delete/2`; verify `mix test test/cluster/router_ryw_test.exs` passes with tests proving the table survives when the first writing process exits and that `prune_ryw/1` removes only expired entries.
- [x] 1.3 Update `Router.local_batch_write/2` in `lib/cluster/router.ex` and `Replicator.apply_op_batch/3` in `lib/cluster/replicator.ex` to insert `:put` record lists with a single `Storage.put(records, partition)` call instead of `Enum.each`; verify `mix test test/distributed/batch_write_test.exs` passes.
- [x] 1.4 Document nested match-binding behavior and RYW table supervision in `lib/storage/match_spec.ex` and `lib/cluster/router.ex` moduledocs; verify `mix compile --warnings-as-errors` succeeds.

## 2. Queue and Stack Concurrency, Bounded Spin-Wait, and Crash Recovery

- [x] 2.1 Refactor `SuperCache.Stack` (`lib/api/stack.ex`) to enforce `@max_spin_retries` (`100`) across `local_push`, `local_pop`, `local_drain`, `dist_do_push`, `dist_do_pop`, and `dist_do_drain`, acquire a `{self(), monotonic_ms}` lock via `Storage.insert_new/2` before taking/modifying the counter, preserve `{{:stack, :counter, name}, 0}` on empty pop/drain, reclaim locks held by dead or timed-out processes (`@lock_stale_ms`), and release locks in `try ... after`; verify `mix test test/stack_test.exs` passes with new tests for exhausted spin retries, dead-holder lock reclamation, empty pop/push cycles, and concurrent pushes to a new stack.
- [x] 2.2 Refactor `SuperCache.Queue` (`lib/api/queue.ex`) to acquire the `{self(), monotonic_ms}` lock via `Storage.insert_new/2` before taking/modifying `:head`/`:tail` counters, reclaim locks from dead or timed-out processes, and wrap all locked critical sections in `try ... after`; verify `mix test test/queue_test.exs` passes with new tests for concurrent enqueue/dequeue on a newly initialized queue and lock recovery after a simulated holder crash.
- [x] 2.3 Update `SuperCache.Queue` and `SuperCache.Stack` moduledocs to describe bounded spin retries and stale-lock recovery; verify `mix docs` builds without warnings.

## 3. KeyValue and Struct Hot-Path Performance and Consistency

- [x] 3.1 Replace full-record materialization in `KeyValue.local_count/1`, `KeyValue.local_keys/1`, and `KeyValue.local_values/1` (`lib/api/key_value.ex`) with `:ets.select_count/2` and projected `:ets.select/2` match specs; verify `mix test test/kv_test.exs` passes for both `:set` and `:bag` table types.
- [x] 3.2 Update `KeyValue.remove_batch/2` in `lib/api/key_value.ex` to route a single `local_delete_batch/2` call per `kv_name` partition in distributed mode instead of per-key RPCs; verify `mix test test/kv_test.exs test/distributed/key_value_test.exs` passes with a test confirming batch removal across multiple keys.
- [x] 3.3 Cache initialized `Struct` key fields in `:persistent_term` in `lib/api/struct.ex` (clearing cached entries on `Bootstrap.stop/0` and `Cluster.Bootstrap.stop/0`) and update distributed `Struct.remove/1` to execute lookup-and-delete atomically on the primary via `local_remove/1` without pre-reading the local replica; verify `mix test test/struct_test.exs test/distributed/struct_test.exs` passes with a test where the local replica is missing the struct record that exists on the primary.
- [x] 3.4 Update `KeyValue` and `Struct` docs in `lib/api/key_value.ex` and `lib/api/struct.ex` to reflect batch removal and primary-consistent struct deletion; verify `mix compile --warnings-as-errors` succeeds.

## 4. Write Buffer (`lazy_put`) FIFO Ordering and Batch Flushing

- [x] 4.1 Update `SuperCache.Internal.Queue` (`lib/buffer/queue.ex`) to reverse buffered items (`:lists.reverse(data)`) when delivering batches to readers so enqueue order is preserved; verify `mix test test/internal_queue_test.exs test/buffer_test.exs` passes with a test asserting sequential updates to the same key within one drained batch retain the latest value.
- [x] 4.2 Update `SuperCache.Internal.Stream` (`lib/buffer/stream.ex`) to flush drained queue batches via `SuperCache.put_batch!/1` with a per-item `push/1` fallback when a batch contains a malformed tuple; verify `mix test test/buffer_test.exs` passes with tests for multi-item batch flushing and malformed-tuple isolation.
- [x] 4.3 Update `SuperCache.Buffer` and `SuperCache.Internal.Stream` moduledocs to document FIFO batch flushing and error isolation; verify `mix compile --warnings-as-errors` succeeds.

## 5. Distributed Consistency, WAL Deduplication, Health, and Handoff

- [x] 5.1 Deduplicate replica acknowledgements in `SuperCache.Cluster.WAL.ack/2` (`lib/cluster/wal.ex`) using `:ets.insert_new(@ack_table, {{seq, :ack, replica_node}, true})` and clean up per-node ack rows on commit or timeout; verify `mix test test/wal_test.exs` passes with a test proving duplicate acks from a single replica cannot satisfy a 3-node majority.
- [x] 5.2 Propagate sync and strong write failures as `%SuperCache.Cluster.Error{}` through `Router`, `DistributedHelpers.apply_write/3`, `DistributedHelpers.route_write/4`, and public write APIs (`SuperCache`, `KeyValue`, `Queue`, `Stack`, `Struct`); verify `mix test test/cluster_local_test.exs test/distributed_helpers_test.exs test/super_cache_test.exs` passes and bang/non-bang failure contracts hold.
- [x] 5.3 Update `Router.do_read/4` and `DistributedHelpers.read_primary/4` / `read_quorum/4` to fall back to local reads only when `DistributedHelpers.has_partition?/1` is true and raise `%SuperCache.Cluster.Error{}` otherwise; verify `mix test test/distributed_helpers_test.exs test/cluster_local_test.exs` covers reachable primary, unreachable primary with local replica, and unreachable primary without local replica.
- [x] 5.4 Update `HealthMonitor` (`lib/cluster/health_monitor.ex`) to evaluate only local-node checks in `run_health_checks/1`, aggregate remote `local_node_health/0` via `Rpc.call/5` with cached degraded/unknown fallback, skip checks before `Config.get_config(:started, false)` is true, and use unique `{:health_probe, node(), make_ref()}` probes cleaned up in `after` blocks; verify `mix test test/health_monitor_deep_test.exs` passes.
- [x] 5.5 Update `Manager` (`lib/cluster/manager.ex`), `Replicator.push_partition/2` (`lib/cluster/replicator.ex`), and `Cluster.Bootstrap.validate!/1` (`lib/cluster/cluster_bootstrap.ex`) to enforce single-donor partition handoff with ETS in-flight deduplication, streaming batch transfer with retry and exact counts, and bootstrap validation of `replication_factor >= 1` and `num_partition > 0`; verify `mix test test/cluster_bootstrap_lifecycle_test.exs test/cluster_local_test.exs` passes.
- [x] 5.6 Document the WAL deduplication, write/read failure propagation, node-local health aggregation, and single-donor handoff behavior in `guides/Distributed.md`; verify `mix docs` completes cleanly.

## 6. Integration and Benchmark Verification

- [x] 6.1 Run the full unit test suite (`mix test`) and cluster test suite (`mix test.cluster`); verify all tests pass with zero warnings or regressions.
- [x] 6.2 Run `mix format --check-formatted` and `mix credo --strict`; verify both exit cleanly.
- [x] 6.3 Run `mix run benchmarks/ets_benchmark.exs` and `mix run benchmarks/distributed_benchmark.exs`; verify no throughput or latency regressions against `benchmarks/results/baseline.md`.

## Workflow follow-up

- Archive the change (`openspec archive improve-performance-stability-consistency`) after all tasks and verification checks pass.
- Reconcile or archive `harden-distributed-reliability` once its overlapping distributed tasks are completed by this change.
