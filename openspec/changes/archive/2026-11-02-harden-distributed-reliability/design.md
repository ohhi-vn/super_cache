# Design

## Context

See `proposal.md` for the motivation. The current distributed stack has five implementation facts that shape this design:

1. `Router` and `DistributedHelpers` execute `:strong` writes through `ThreePhaseCommit`, while `Replicator` contains a separate WAL path that the public write path never reaches. Documentation and benchmarks describe the WAL path.
2. Replication results are frequently discarded: `Router.local_write/3` returns `true` after `ThreePhaseCommit.commit/2`, and `DistributedHelpers.apply_write/3` returns `:ok` for non-strong modes regardless of replica outcomes.
3. Quorum waiting is implemented three different ways. `Replicator.await_quorum/3` can loop forever when all tasks time out, `Router.quorum_await/3` polls every 100 ms, and `DistributedHelpers.read_quorum/4` waits per task and drops `nil` results.
4. Async replication uses `Task.Supervisor.async_nolink/2` without awaiting the task, so every write leaves a task-result message in the caller's mailbox.
5. `HealthMonitor` runs checks for remote nodes in the local process and writes probe records to local ETS tables, so remote health data is not trustworthy.

The cluster tests use `:peer` and run under `mix test.cluster`; the normal `mix test` excludes them. Dependencies are not installed in the current checkout, so the implementation must start with `mix deps.get`.

## Goals / Non-Goals

**Goals:**

- One strong-write protocol for every public data structure.
- Quorum primitives with a hard deadline, early termination, duplicate suppression, and predictable cleanup.
- A documented distributed error type that carries operation, partition, and node context.
- Health data that reflects the node that owns the data.
- Partition handoff that is deterministic, streamed, retried, and accurately reported.
- Measurable improvement or no regression on the existing local benchmark and a new distributed benchmark baseline.

**Non-Goals:**

- Durable on-disk WAL or transaction log across a full VM restart. The WAL remains in memory; a separate store process is added so it survives a replication-process crash, not a VM restart.
- Raft, Paxos, or general multi-key distributed transactions.
- Changing the partition-assignment algorithm or the public API signatures.
- Detecting network partitions beyond Erlang node connectivity events.
- Full anti-entropy repair for arbitrary concurrent writes during handoff. Handoff remains best-effort with retries and explicit reporting.

## Decisions

### 1. WAL is the only strong-write implementation

`Router`, `DistributedHelpers`, and every strong branch in `KeyValue`, `Queue`, `Stack`, and `Struct` will call `WAL.commit/2`. `ThreePhaseCommit` stays in the tree as a legacy internal module so callers that reference it continue to compile, but the public write path no longer selects it. Its module docs will state that it is deprecated and not used by SuperCache.

`WAL.commit/2` will:

1. Validate the partition and operation list before touching storage.
2. Reject the write with `{:error, :wal_backpressure}` when pending entries are at the configured `max_pending`.
3. Append a `:pending` entry with a monotonically increasing sequence and timestamp.
4. Apply the operations locally, then ask every replica to apply them.
5. Count the primary's local apply plus each successful replica as one vote; require a strict majority of all copies.
6. Delete the entry on success. On failure, mark it `:failed` and retain it for a bounded retry/recovery window.
7. Return `:ok` or a structured error; callers decide whether to raise.

The alternative was to keep 3PC and fix its quorum behavior. That would contradict the documented strong-mode model, keep two recovery mechanisms, and preserve the slower round-trip profile. Unifying on WAL also lets batch writes use one quorum round trip.

For compatibility with a mixed-version cluster, the new coordinator calls the existing `WAL.replicate_and_ack/3` entry point through `:erpc.call/5` and treats `:ok` as an acknowledgement. That function already exists on older nodes and still sends the legacy ack cast, so an old primary can continue to use the old ack path. Strong mode still requires all nodes to run the new protocol for consistent guarantees; async and sync modes can tolerate a rolling upgrade.

### 2. A dedicated WAL store process owns the tables

Today the WAL ETS tables are owned by the WAL GenServer. If that GenServer crashes, the tables disappear and recovery is impossible. A small `SuperCache.Cluster.WAL.Store` child will own the WAL and acknowledgement tables, and the WAL GenServer will use them. The store starts before WAL in the application supervision tree. This satisfies the "survives a replication-component restart" requirement without adding disk I/O.

The alternative, `:dets` or `:disk_log`, would provide VM-restart durability but adds fsync cost, file lifecycle, and a new operational concern. It is deliberately deferred.

### 3. One bounded quorum helper

Add `SuperCache.Cluster.Quorum` with a single API that takes a list of `{node, task_fun}` entries, a required vote count, and a timeout. It starts tasks under a dedicated `Task.Supervisor`, then uses selective receive on task results and monitor messages. It returns `{:ok, value, voters}` or `{:error, reason, results}`. On early success or deadline it shuts down and flushes every remaining task.

This replaces:

- `Replicator.await_quorum/3`, which can recurse forever when `Task.yield_many/2` returns only `nil` results.
- `Router.quorum_await/3`, which polls every 100 ms without an overall deadline.
- `DistributedHelpers.await_quorum/7`, which calls `Task.yield/2` sequentially and drops `nil`.

Values are stored in a map without filtering `nil`, so `nil` can win a quorum. Duplicate acknowledgements are impossible because the helper receives one task result per node, and the WAL additionally keys acknowledgements by `{seq, node}` for the legacy path.

### 4. Normalize remote errors at the RPC boundary

Add `SuperCache.Cluster.Error` (an exception) and `SuperCache.Cluster.Rpc`:

- `Rpc.call/5` returns `{:ok, value}` or `{:error, %Error{}}`.
- `Rpc.call!/5` raises the error.
- `Rpc.normalize/2` converts `:nodedown`, `:timeout`, `{:erpc, reason}`, `{:exception, class, reason, stack}`, and non-exception reasons into `Error` with `operation`, `partition`, `node`, `mode`, and `reason` fields.

`SuperCache.safe/1` will catch `:exit` and `:throw` in addition to `rescue`, wrapping non-exception reasons. This makes the existing "non-bang never raises" contract true.

### 5. Read paths fail loudly or fall back explicitly

`Router.do_read(:primary)` and `DistributedHelpers.read_primary/4` will:

- Use `Rpc.call/5` to the primary.
- On failure, check `has_partition?/1`. If the local node holds a replica, read locally, log a warning, and increment a fallback counter. If it does not, raise `SuperCache.Cluster.Error`.

Quorum reads use `Quorum.await/4` with `local_read/3` as the task function. If no quorum forms, the read falls back to the primary result when the primary is reachable; otherwise it raises. Every fallback increments a counter so operators can see degraded reads.

The current `Router.quorum_await/3` and `DistributedHelpers.await_quorum/7` are removed.

### 6. Async replication uses supervised fire-and-forget workers

`Replicator.do_replicate(:async, ...)` will use `Task.Supervisor.start_child/2` with a wrapper that rescues/catches all failures, logs them, and increments a counter. It will not use `async_nolink/2`, which would leave a result message in the caller's mailbox. The pool gets a configurable `max_children`; if the pool is full, the write still succeeds locally, a drop counter increases, and the failure is logged. No synchronous blocking is added to the async path.

### 7. Health checks are node-local and aggregated

`HealthMonitor.run_health_checks/1` will check only `node()`:

- Connectivity to live peers.
- Local ETS partition counts for partitions this node owns.
- Local metrics for error rate.
- Replication lag for a sample of local primary partitions.

`cluster_health/0` will aggregate `local_node_health/0` from every live node through `Rpc.call/5` with a deadline, falling back to the last locally stored result for unreachable nodes. A remote node's health never uses the local node's tables.

Replication-lag probes use `{:health_probe, node(), make_ref()}`. The probe is written through the configured replication path, polled on the replicas, and removed in an `after` block on both the primary and every replica. Health cycles run under a deadline; timed-out checks store `:unhealthy` (connectivity) or `:unknown` (other checks) and increment a timeout counter. The monitor skips checks until `Config.get_config(:started, false)` is true.

### 8. Single-donor, streamed handoff

`Manager` gets a supervised task pool and an ETS transfer table:

- `sync_to_node/1` computes the target's assignments in the current map. For each partition, the deterministic donor is the primary if it is alive and not the target, otherwise the first live replica that is not the target. Only the donor node sends. Other holders skip.
- `reconcile_ownership/2` uses the same donor rule against the old map for partitions this node newly holds.
- `:ets.insert_new/2` on `{partition_idx, target}` coalesces duplicate in-flight transfers.
- `Replicator.push_partition/2` folds the table in bounded batches instead of building a full list, retries transient batch failures with bounded backoff, and returns exact `transferred` and `failed` counts. A failed batch does not discard successful batches.
- Handoff and transfer outcomes increment counters visible in `Cluster.Stats`.

### 9. Fast config keys and cheaper metrics

Add `:replication_mode`, `:replication_factor`, and `:started` to `Config`'s fast-key set. Today every distributed write calls `Manager.replication_mode/0`, which performs a `GenServer.call/3`; this is a per-write serialization point.

`Metrics.push_latency/2` will write into a fixed-size ETS ring buffer from the caller instead of casting every sample to the Metrics GenServer. The buffer is read by slot, so memory stays bounded and the Metrics process is no longer a throughput bottleneck. Counter writes already use atomic `:ets.update_counter/4`.

New counters: async replication failures/drops, sync and strong quorum failures, WAL backpressure, read fallbacks, health timeouts, handoff successes/failures.

### 10. Batch and increment semantics

`Router.local_batch_write/2` under `:strong` will call `WAL.commit/2` once with the whole batch instead of one commit per record. A batch that spans partitions produces one commit per partition and aggregates failures in the returned error.

`KeyValue.increment/4` under `:strong` will use a deterministic `{:increment, key, default, step}` operation applied with `:ets.update_counter/4` on every copy, instead of a read-modify-write followed by a put. This makes retries safe and removes the TOCTOU race. Bag inserts remain non-idempotent; strong-mode recovery documentation will call that out.

## Risks / Trade-offs

- **In-memory WAL is not durable across a VM restart.** → A separate store process survives WAL GenServer crashes; docs will state the VM-restart boundary. Disk persistence is a follow-up.
- **A failed strong write may leave partial state on the primary or some replicas.** → Retries are safe for put/delete and deterministic increment; failed entries are marked and visible. Full rollback would require a multi-phase protocol and is out of scope.
- **Mixed-version clusters have different strong-mode implementations.** → Async/sync remain rolling-upgrade safe. Strong mode requires a coordinated upgrade; `Cluster.Bootstrap` config verification can warn when peer versions differ, and release notes will call it out.
- **Quorum helper uses a dedicated Task.Supervisor.** → Adds one supervised process and a small amount of message traffic, but removes unbounded waits and leaked tasks.
- **Health aggregation adds RPC fan-out.** → Bounded deadline and cached last-known values prevent a slow node from blocking the report.
- **Single-donor handoff can serialize large joins.** → Transfer table coalesces duplicates; a bounded number of partition transfers run concurrently. A full parallel handoff can be tuned later.
- **Error propagation changes observable behavior.** → Writes that previously returned success while dropping replication errors now raise or return an error. This matches the documented contract; release notes and tests will make the change explicit.
- **Benchmarks are environment-sensitive.** → Record medians and use tolerance bands; do not make CI fail on small variance.

## Migration Plan

1. Add the new error, RPC, quorum, WAL store, and fast-key infrastructure without changing call sites.
2. Switch strong writes to WAL behind the existing `:replication_mode` option; keep the legacy 3PC functions for source compatibility.
3. Update read, health, and membership paths.
4. Deploy all nodes in a strong-mode cluster together. Async and sync clusters may roll one node at a time.
5. Rollback is a code revert and a cluster restart; there is no persistent schema or file format to migrate. In-flight strong writes may be lost on rollback, as with any cache restart.
6. Run `mix deps.get`, `mix test`, `mix test.cluster`, `mix format --check-formatted`, `mix credo --strict`, and `mix dialyzer` before release. Run `mix run benchmarks/ets_benchmark.exs` and the new distributed benchmark before and after.

## Open Questions

None that change the specs or approach. The only deferred decision is whether to add an optional disk-backed WAL; that would be a separate change and does not affect this design's in-memory recovery contract.
