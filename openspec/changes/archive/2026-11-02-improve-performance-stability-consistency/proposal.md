# Proposal

## Why

While core ETS partitioning and basic WAL/quorum primitives are in place, several critical performance bottlenecks, crash-stability hazards, and consistency bugs remain across both local and distributed execution paths. Under concurrent load or partial failures, `Queue` and `Stack` can corrupt state or spin indefinitely, `Router`'s read-your-writes ETS table can vanish when a short-lived caller exits, `Storage` and `KeyValue` copy full records or issue N network hops where single ETS/RPC operations suffice, and `WAL`/distributed routing still allow duplicate replica acks or silent write/read failures.

## What Changes

- **Harden `Queue` and `Stack` concurrency and crash stability.** Acquire soft locks before mutating or taking counters to close the `Storage.take` vs `local_init` reset race, enforce bounded spin-wait retries in `Stack` (matching `Queue`), wrap all locked mutations in `try ... after` with timestamped stale-lock recovery so a crashed process never wedges a data structure, and check lock state consistently across distributed drain operations.
- **Optimize `KeyValue` and `Struct` hot paths and consistency.** Replace full-record materialization in `KeyValue.count/2`, `KeyValue.keys/2`, and `KeyValue.values/2` with `:ets.select_count/2` and projected `:ets.select/2` specs; batch distributed `KeyValue.remove_batch/2` per partition instead of issuing one RPC per key; cache initialized `Struct` key fields in `:persistent_term` (cleared on stop) to eliminate repeated ETS/`:erpc` lookups; and make distributed `Struct.remove/1` execute atomically on the primary without a stale local pre-read.
- **Fix `Storage` match binding extraction and batch ETS writes.** Support nested tuple patterns and numeric capture ordering (`:"$1"`, `:"$2"`, ...) in `Storage.get_by_match/2`, and replace per-record `Enum.each` ETS inserts in `Router.local_batch_write/2` and `Replicator.apply_op_batch/3` with single-call batch `Storage.put/2` operations.
- **Supervise the Read-Your-Writes tracker and optimize pruning.** Move ownership of `SuperCache.Cluster.Router.RywTracker` to a permanent supervised process so short-lived caller processes cannot destroy the table on exit or race during `:ets.new/2`, and replace full-table `:ets.foldl/3` pruning with `:ets.select_delete/2`.
- **Preserve FIFO order and batch writes in `Buffer` (`lazy_put`).** Deliver queued items in FIFO order per scheduler queue and flush drained batches via `SuperCache.put_batch!/1` instead of one `SuperCache.put/1` call per tuple.
- **Complete distributed consistency and observability guarantees.** Deduplicate `WAL.ack/2` votes by `{seq, replica_node}`, propagate sync/strong write and read failures as `SuperCache.Cluster.Error` (raising on bang APIs, returning `{:error, exception}` on non-bang APIs), restrict primary-read fallback to nodes that actually hold a replica (`has_partition?/1`), scope `HealthMonitor` checks to local node data with unique cleaned-up probes, and enforce single-donor streamed partition handoffs.

## Capabilities

### New Capabilities
- `data-structures`: Concurrency safety, bounded spin-waiting, crash-resilient soft locking, and hot-path query/batch optimizations for `KeyValue`, `Queue`, `Stack`, and `Struct`.
- `storage-and-query-engine`: Nested match-spec binding extraction, atomic batch ETS writes, supervised Read-Your-Writes table lifecycle, and match-spec-based RYW pruning.
- `write-buffer`: FIFO ordering and batched cache flushing for per-scheduler `lazy_put/1` buffer streams.
- `distributed-consistency`: Per-replica WAL acknowledgement deduplication, end-to-end distributed write/read error propagation, replica-aware primary read failover, node-local health monitoring, and single-donor streaming partition handoff.

### Modified Capabilities
<!-- No existing specs in openspec/specs/. -->

## Impact

- **Code**: `lib/api/{super_cache,key_value,queue,stack,struct}.ex`, `lib/storage/{storage_api,match_spec}.ex`, `lib/buffer/{buffer,queue,stream}.ex`, `lib/cluster/{router,distributed_helpers,replicator,wal,wal/store,manager,health_monitor,cluster_bootstrap,stats}.ex`.
- **Public APIs**: Preserves all existing function signatures and success return shapes. Distributed failure paths that previously returned silent success or `[]` now raise `SuperCache.Cluster.Error` (bang functions) or return `{:error, %SuperCache.Cluster.Error{}}` (non-bang functions).
- **Performance**: Reduces memory allocation and ETS copying on `KeyValue` scans/counts, eliminates N-hop RPCs in `KeyValue.remove_batch/2` and `Struct` operations, batches `lazy_put/1` flushes and replica batch writes, and replaces O(N) RYW table scans with `:ets.select_delete/2`.
- **Stability**: Eliminates infinite spin loops in `Stack`, permanent deadlocks after mid-operation process crashes in `Queue`/`Stack`, and `:badarg`/table-loss crashes in `Router.RywTracker`.
