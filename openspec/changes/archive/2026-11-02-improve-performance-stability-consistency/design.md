# Design

## Context

See `proposal.md` for motivation and `specs/*/spec.md` for the behavioral contracts. Eight concrete implementation realities in the current codebase drive this design:

1. `SuperCache.Stack` (`lib/api/stack.ex`) has no `@max_spin_retries` limit on `local_push`, `local_pop`, `local_drain`, `dist_do_push`, or `dist_do_pop`, and `dist_do_drain` does not check `{:stack, :updating, name}`. Furthermore, both `Queue` (`lib/api/queue.ex`) and `Stack` call `Storage.take` on `{:queue, :tail, name}` or `{:stack, :counter, name}` *before* setting the `{:updating, name}` lock; a concurrent caller observing `take -> []` and `updating -> []` runs `local_init` and resets the counter to `0` mid-operation. Neither module releases its lock in a `try ... after` block or expires locks left by dead processes.
2. `SuperCache.KeyValue` (`lib/api/key_value.ex`) implements `local_count/1`, `local_keys/1`, and `local_values/1` by calling `Storage.get_by_match_object({{:kv, kv_name, :_}, :_}, partition)` and mapping/counting in Elixir, copying every stored value out of ETS. `KeyValue.remove_batch/2` in distributed mode loops over `keys` with individual `Router.route_delete_by_key_partition!/2` RPCs.
3. `SuperCache.Struct` (`lib/api/struct.ex`) queries ETS (and falls back to `:erpc.call` to the primary) via `get_key_field/1` on every operation, and `Struct.remove/1` in distributed mode calls `get/1` (default `:local` read mode) before routing `local_remove`, failing with `{:error, :not_found}` when a local replica lags behind the primary.
4. `SuperCache.Storage.get_by_match/2` (`lib/storage/storage_api.ex`) extracts bindings with `Tuple.to_list(pattern)` and `Atom.to_string/1`, which misses capture variables inside nested tuples (such as `{{:kv, ns, :"$1"}, :"$2"}`) and fails to sort out-of-order variables (`:"$2"` before `:"$1"`).
5. `Router.ensure_ryw_table/0` (`lib/cluster/router.ex`) creates `SuperCache.Cluster.Router.RywTracker` lazily in whichever caller process first runs a distributed operation; when that caller is short-lived, the ETS table is destroyed on process exit. `Router.prune_ryw/1` folds over the entire ETS table with `:ets.foldl/3`.
6. `SuperCache.Internal.Queue` (`lib/buffer/queue.ex`) prepends incoming items (`[item | data]`) and sends the unreversed list to waiting readers, inverting write order within a batch, while `SuperCache.Internal.Stream` (`lib/buffer/stream.ex`) calls `SuperCache.put/1` one tuple at a time instead of flushing drained batches via `SuperCache.put_batch!/1`.
7. `SuperCache.Cluster.WAL.ack/2` (`lib/cluster/wal.ex`) ignores `replica_node` and increments `{seq, :count}` unconditionally, allowing duplicate acknowledgements from a single replica to satisfy quorum early.
8. Distributed write/read failure propagation (`Router`, `DistributedHelpers`, public APIs), node-local `HealthMonitor` checks, and single-donor streaming handoff in `Manager`/`Replicator` remain only partially wired.

## Goals / Non-Goals

**Goals:**

- Eliminate lock-acquisition races, unbounded busy-wait loops, and permanent lock leaks in `Queue` and `Stack`.
- Avoid unnecessary ETS data copying and N-hop RPC loops in `KeyValue`, `Struct`, `Storage`, `Router`, and `Replicator`.
- Ensure `SuperCache.Cluster.Router.RywTracker` is permanently owned by a supervised process and pruned in ETS via match-delete.
- Guarantee FIFO batch ordering and batched flushing in the `Buffer` (`lazy_put`) pipeline.
- Complete the distributed consistency path: deduplicated WAL replica acks, normalized `SuperCache.Cluster.Error` propagation on write/read failures, replica-checked primary read fallback, node-local health checks, and single-donor streamed partition handoffs.

**Non-Goals:**

- Replacing ETS-backed `Queue` and `Stack` with dedicated per-queue GenServers (which would change the lock-free/partitioned architecture).
- On-disk persistence for WAL or cache partitions across VM restarts.
- Changing any public function signatures or successful return value shapes.

## Decisions

### 1. Lock-first protocol with owner/timestamp lease and `try ... after` in `Queue` and `Stack`

All mutating operations in `Queue` and `Stack` (both local and distributed primary paths) will acquire the per-structure lock record `{{tag, :updating, name}, {self(), monotonic_ms}}` via `Storage.insert_new/2` *before* reading, taking, or initializing `{:queue, :head/:tail, name}` or `{:stack, :counter, name}`:

- **Acquire with bounded spin and stale-lock reclamation:** A shared helper attempts `Storage.insert_new({{tag, :updating, name}, {self(), now}}, partition)`. If present, it inspects `{holder_pid, locked_at}`: if `not Process.alive?(holder_pid)` or `now - locked_at > @lock_stale_ms` (e.g., `2_000` ms), it overwrites the stale lock and proceeds; otherwise it yields and retries up to `@max_spin_retries` (`100`).
- **Guaranteed release:** Once acquired, the critical section runs inside `try ... after Storage.delete({tag, :updating, name}, partition) end`.
- **Counter preservation on empty pop/drain:** When `Stack` is empty (`counter == 0` or missing), `local_pop` and `local_drain` keep `{{:stack, :counter, name}, 0}` intact instead of leaving the counter deleted.

*Alternatives considered:*
- Partition-level GenServer locks: rejected because unrelated keys/queues in the same partition would serialize behind a single process.
- Keeping `Storage.take` before locking: rejected because the window between `take` and `lock` cannot distinguish "uninitialized" from "counter temporarily taken by an active writer".

### 2. Recursive numeric binding extraction in `MatchSpec` and projected `KeyValue` selects

`SuperCache.Storage.MatchSpec` will provide `extract_bindings/1` that recursively traverses tuples and lists in `pattern`, matches capture atoms (`:"$1"`..`:"$99"` or any `:"$<n>"`), deduplicates them, and sorts them by integer index:

- `Storage.get_by_match/2` uses this helper so nested tuple patterns (e.g., `{{:kv, ns, :"$1"}, :"$2"}`) return `[b1, b2]` in `:ets.match/2` order.
- `KeyValue.local_count/1` uses `:ets.select_count(partition, [{{{:kv, kv_name, :_}, :_}, [], [true]}])`.
- `KeyValue.local_keys/1` uses `:ets.select(partition, [{{{:kv, kv_name, :"$1"}, :_}, [], [:"$1"]}]) |> Enum.uniq()`.
- `KeyValue.local_values/1` uses `:ets.select(partition, [{{{:kv, kv_name, :_}, :"$1"}, [], [:"$1"]}])`.
- `KeyValue.remove_batch/2` in distributed mode routes a single `local_delete_batch(kv_name, keys)` call via `DistributedHelpers.route_write/4` that builds `Enum.map(keys, &{:delete, {:kv, kv_name, &1}})` and applies it in one `apply_write/3` batch.

*Alternatives considered:*
- Calling `:ets.match/2` directly in `Storage.get_by_match/2`: `:ets.select/2` with a match spec is already standardized across `MatchSpec` and avoids pattern re-parsing when specs are reused; fixing `extract_bindings/1` keeps `MatchSpec.match/2` accurate for all callers.

### 3. `:persistent_term` cache for `Struct` key-field metadata and primary-routed `Struct.remove/1`

`SuperCache.Struct` will cache `{SuperCache.Struct, :key_field, struct_name} -> key` in `:persistent_term` whenever `init/2` or `get_key_field/1` resolves the key field, and erase cached struct key entries when `Bootstrap.stop/0` / `Cluster.Bootstrap.stop/0` runs:

- `Struct.remove/1` in distributed mode will no longer call `get(struct)` on the caller node before routing. Instead, `DistributedHelpers.route_write(__MODULE__, :local_remove, [struct], partition_idx)` executes on the primary, where `local_remove/1` looks up the existing struct in the primary's ETS partition, deletes and replicates the removal if present, and returns `{:ok, existing}` or `{:error, :not_found}`.

*Alternatives considered:*
- Escalating the pre-read in `Struct.remove/1` to `read_mode: :primary`: rejected because it requires two network round-trips (one primary read + one routed write) and still leaves a TOCTOU window between the read and delete.

### 4. Supervised `RywTracker` table ownership and `:ets.select_delete/2` pruning

`SuperCache.Cluster.WAL.Store` (which starts in the application supervision tree before cluster traffic and already owns cluster ETS tables) or `SuperCache.EtsHolder` will create and own `SuperCache.Cluster.Router.RywTracker` at application startup (`:set`, `:public`, `:named_table`, `{:read_concurrency, true}`, `{:write_concurrency, true}`). `Router.ensure_ryw_table/0` becomes a fast check that falls back to creating the table only if called outside the normal supervision tree in isolated unit tests.

`Router.prune_ryw/1` will replace `:ets.foldl/3` with:
```elixir
:ets.select_delete(@ryw_table, [{{{self(), :_}, :"$1"}, [{:<, :"$1", now}], [true]}])
```
which executes the predicate inside the BEAM ETS engine in C without copying non-matching rows to the calling process.

*Alternatives considered:*
- Process dictionary for RYW tracking: works for same-process calls, but existing tests and `Router.ryw_recent?/1` inspect `@ryw_table` across helper calls and expect an ETS table. Keeping the supervised ETS table preserves full test compatibility while fixing ownership and pruning cost.

### 5. FIFO batching in `Internal.Queue` and `Internal.Stream`

- In `SuperCache.Internal.Queue`, when delivering `data` to a reader (`loop([reader | rest_readers], [_ | _] = data, name)`), send `:lists.reverse(data)` so items are emitted in FIFO arrival order.
- In `SuperCache.Internal.Stream.create/1`, yield each drained batch as a single list element `{[items], q}` (or chunk in `make_stream_pipe/1`), and flush the batch with `SuperCache.put_batch!/1`. If `put_batch!/1` raises (for instance, due to one malformed tuple in the batch), fall back to per-item `push/1` for that batch so valid tuples still succeed and only the bad tuple is logged.
- In `Router.local_batch_write/2` and `Replicator.apply_op_batch/3` (for `:put`), pass the list of tuples directly to `Storage.put(records, partition)` (`:ets.insert/2` natively inserts a list of tuples atomically) instead of `Enum.each`.

*Alternatives considered:*
- Keeping single-item stream elements and using `Stream.chunk_every/2`: `Internal.Queue.get/1` already returns the exact batch of items accumulated since the last read; flushing that batch directly avoids extra stream chunking overhead and latency timers.

### 6. WAL per-replica ack deduplication and completion of distributed write/read/health/handoff contracts

- **WAL deduplication:** In `SuperCache.Cluster.WAL.ack/2`, record each replica's acknowledgement with `:ets.insert_new(@ack_table, {{seq, :ack, replica_node}, true})` before incrementing `{seq, :count}`. Only when `insert_new` returns `true` is the counter incremented. Clean up `{{seq, :ack, :_}, :_}` rows via `:ets.match_delete/2` when majority is reached or on timeout.
- **Write and read failure propagation:** Complete the wiring in `Router`, `DistributedHelpers`, and public APIs (`SuperCache`, `KeyValue`, `Queue`, `Stack`, `Struct`) so `Rpc.call/5` / `Rpc.call!/5` and `WAL.commit/2` / `Replicator.replicate/3` errors are raised as `%SuperCache.Cluster.Error{}` in bang functions and returned as `{:error, %SuperCache.Cluster.Error{}}` in non-bang functions. In `DistributedHelpers.apply_write/3` for `:sync` mode, collect operations or check each `Replicator.replicate/3` result and return `{:error, reason}` when any replica quorum fails.
- **Primary read failover:** Both `Router.do_read(:primary, ...)` and `DistributedHelpers.read_primary/4` check `DistributedHelpers.has_partition?(order)` on RPC failure; if true, they read locally and increment `Metrics.increment(:read, :fallbacks)`, otherwise they raise `SuperCache.Cluster.Error`.
- **Node-local `HealthMonitor` and single-donor `Manager` handoff:** `HealthMonitor` evaluates local checks on `node()`, aggregates peer health via `Rpc.call/5` with cached fallback for unreachable nodes, and uses unique `{:health_probe, node(), make_ref()}` probes cleaned up in `after` blocks. `Manager` selects one deterministic donor per partition, coalesces in-flight transfers in an ETS table, and streams batches in `Replicator.push_partition/2` with retry and exact counts.

## Risks / Trade-offs

- **[Stale lock reclamation could overlap a paused process]** → Set `@lock_stale_ms` to `2_000` ms (orders of magnitude longer than an in-memory ETS operation, which takes microseconds) and check `Process.alive?(holder_pid)` first so dead holders are reclaimed immediately.
- **[`:persistent_term` updates for `Struct` key fields trigger GC if mutated frequently]** → `Struct.init/2` is called once per struct module at setup; writes only happen when the key is not yet in `:persistent_term`, and `Bootstrap.stop/0` only erases keys when tearing down the cache.
- **[Batch fallback on malformed tuple in `Buffer`]** → In the happy path, `SuperCache.put_batch!/1` writes the entire drained batch at once; only when a batch contains an invalid tuple does it retry item-by-item, preserving both peak throughput and fault isolation.
- **[Stricter error propagation surfaces previously hidden replication failures]** → This aligns runtime behavior with the documented failure contract; unit and cluster tests will verify both success compatibility and failure reporting.

## Migration Plan

1. Implement storage match-binding extraction, supervised `RywTracker` ownership, and `MatchSpec`-based RYW pruning.
2. Harden `Queue` and `Stack` locking, spin limits, and empty-counter handling; optimize `KeyValue` projections/batch delete and `Struct` metadata caching/primary remove.
3. Update `Internal.Queue` and `Internal.Stream` for FIFO batch delivery and `put_batch!/1` flushing, and switch `Router`/`Replicator` batch puts to single `Storage.put/2` list inserts.
4. Complete WAL per-replica ack deduplication, `Router`/`DistributedHelpers` write/read error propagation, `HealthMonitor` node-local aggregation and probe cleanup, and `Manager`/`Replicator` single-donor streaming handoff.
5. Run `mix test`, `mix test.cluster`, `mix format --check-formatted`, `mix credo --strict`, and benchmarks (`benchmarks/ets_benchmark.exs` and `benchmarks/distributed_benchmark.exs`) to verify zero regressions.
