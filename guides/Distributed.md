# Distributed Cache Guide

SuperCache supports distributing data across a cluster of Erlang nodes with configurable consistency guarantees, automatic partition assignment, and continuous health monitoring.

> **Note**: Distributed mode is production-ready but still evolving. Always test with your specific workload before deploying to critical systems.

## Cluster Architecture

SuperCache uses a primary-replica model with hash-based partition assignment:

```
┌─────────────────────────────────────────────────────────────┐
│                        Cluster                               │
│                                                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐      │
│  │  Node A     │    │  Node B     │    │  Node C     │      │
│  │  (Primary)  │◄──►│  (Replica)  │◄──►│  (Replica)  │      │
│  │  P0, P3, P6 │    │  P1, P4, P7 │    │  P2, P5, P8 │      │
│  └─────────────┘    └─────────────┘    └─────────────┘      │
│         │                  │                  │               │
│         └──────────────────┼──────────────────┘               │
│                            │                                  │
│                    Replication (async/sync/WAL)               │
└─────────────────────────────────────────────────────────────┘
```

Partitions are assigned by rotating the sorted node list. With `N` nodes and replication factor `R`, partition `idx` gets:
- **Primary**: `sorted_nodes[idx mod N]`
- **Replicas**: Next `min(R-1, N-1)` nodes in the rotated list

## Configuration

### Basic Setup

All nodes **must** share identical partition configuration:

```elixir
# config/config.exs
config :super_cache,
  auto_start:         true,
  key_pos:            0,
  partition_pos:      0,
  cluster:            :distributed,
  replication_mode:   :async,      # :async | :sync | :strong
  replication_factor: 2,           # primary + 1 replica
  table_type:         :set,
  num_partition:      8            # Must match across ALL nodes
```

### Peer Discovery

Configure cluster peers for automatic connection:

```elixir
# config/runtime.exs
import Config

config :super_cache,
  cluster_peers: [
    :"node1@10.0.0.1",
    :"node2@10.0.0.2",
    :"node3@10.0.0.3"
  ]
```

The application will automatically connect to peers on startup and join the cluster.

### Manual Bootstrap

For dynamic clusters or runtime control:

```elixir
SuperCache.Cluster.Bootstrap.start!(
  key_pos: 0,
  partition_pos: 0,
  cluster: :distributed,
  replication_mode: :strong,
  replication_factor: 2,
  num_partition: 8
)
```

## Replication Modes

SuperCache offers three replication modes, each trading off latency vs. durability:

| Mode | Guarantee | Latency | Use Case |
|------|-----------|---------|----------|
| `:async` | Eventual consistency | ~50-100µs | High-throughput caches, session data, non-critical state |
| `:sync` | Majority ack (adaptive quorum) | ~100-300µs | Balanced durability/performance, user preferences |
| `:strong` | WAL-based strong consistency | ~200µs | Critical data requiring durability, financial state |

### Async Mode

Fire-and-forget replication via a `Task.Supervisor` worker pool. The primary applies the write locally and immediately returns `:ok`. Replication happens in the background.

```elixir
config :super_cache,
  replication_mode: :async
```

**Pros**: Lowest latency, highest throughput
**Cons**: Writes may be lost if primary crashes before replication completes

### Sync Mode (Adaptive Quorum Writes)

Returns `:ok` once a **strict majority** of replicas acknowledge the write. Avoids waiting for slow stragglers while maintaining strong durability guarantees.

```elixir
config :super_cache,
  replication_mode: :sync
```

**How it works**:
1. Primary applies write locally
2. Sends to all replicas via `Task.async`
3. Returns `:ok` as soon as `floor(replicas/2) + 1` acknowledge
4. If majority fails, returns `{:error, :quorum_not_reached}`

**Pros**: Durable without waiting for all replicas, tolerates slow nodes
**Cons**: Slightly higher latency than async

### Strong Mode (WAL-based Consistency)

Replaces the heavy Three-Phase Commit (3PC) protocol with a Write-Ahead Log (WAL) approach. ~7x faster than traditional 3PC (~200µs vs ~1500µs).

```elixir
config :super_cache,
  replication_mode: :strong

# Optional WAL tuning
config :super_cache, :wal,
  majority_timeout: 2_000,  # ms to wait for majority ack
  cleanup_interval: 5_000   # ms between WAL cleanup cycles
  max_pending: 10_000       # max uncommitted entries
```

**How it works**:
1. Write to local ETS immediately (write-ahead)
2. Append operation to WAL (in-memory ETS table with sequence numbers)
3. Async replicate to replicas via `:erpc.cast` (fire-and-forget)
4. Replicas apply locally and ack back via `:erpc.cast`
5. Return `:ok` once majority acknowledges
6. Periodic cleanup removes old WAL entries

**Recovery**: On node restart, `WAL.recover/0` replays any uncommitted entries to ensure consistency.

**Pros**: Strong consistency with low latency, crash-safe, automatic recovery
**Cons**: Slightly more memory for WAL entries, requires majority for writes

## Read Modes

Distributed reads support three consistency levels:

```elixir
# Local read (fastest, may be stale if not yet replicated)
SuperCache.get!({:user, 1})

# Primary read (consistent with primary node)
SuperCache.get!({:user, 1}, read_mode: :primary)

# Quorum read (majority agreement with early termination)
SuperCache.get!({:user, 1}, read_mode: :quorum)
```

### Local Read
Reads from the local node's ETS table. Fastest option (~10µs) but may return stale data if replication hasn't completed.

### Primary Read
Routes the read to the partition's primary node. Guarantees reading the most recent write for that partition. Adds one network round-trip (~100-300µs depending on network).

### Quorum Read (Early Termination)
Reads from all replicas and returns as soon as a **strict majority** agrees on the result. Uses `Task.yield` + selective receive for immediate notification when any task completes — no polling overhead.

**How it works**:
1. Launch `Task.async` for each replica (including primary)
2. Await tasks one-by-one with `Task.yield`
3. Track result frequencies
4. Return immediately when any result reaches quorum
5. Kill remaining tasks to free resources
6. Fall back to primary if no majority is reached

**Pros**: Strong consistency, tolerates slow/unresponsive replicas, early termination saves time
**Cons**: Highest read latency (~100-200µs), more network traffic

### Read-Your-Writes Consistency

The `Cluster.Router` tracks recent writes in an ETS table and automatically forces `:primary` read mode for keys that were recently written, ensuring you always read your own writes.

```elixir
SuperCache.Cluster.Router.track_write(partition_idx)
SuperCache.Cluster.Router.ryw_recent?(partition_idx)
SuperCache.Cluster.Router.resolve_read_mode(mode, partition_idx)
```

## Data Flow

### Write Path (Strong Mode)

```
Client → SuperCache.put!({:user, 1, "Alice"})
  → Router.route_put!
    → Primary.local_put
      → WAL.commit
        → Apply to local ETS
        → Append to WAL
        → :erpc.cast to replicas
          → Replicas.apply_local
          → Replicas.ack back to primary
        → Wait for majority ack
        → Return :ok
```

### Read Path (Quorum Mode)

```
Client → SuperCache.get!({:user, 1}, read_mode: :quorum)
  → Router.do_read(:quorum)
    → Task.async for each replica
    → Await tasks with early termination
    → Return when majority agrees
    → Kill remaining tasks
```

## Performance

### Distributed Latency (Typical LAN)

| Operation | Async | Sync (Quorum) | Strong (WAL) |
|-----------|-------|---------------|--------------|
| Write | ~50-100µs | ~100-300µs | ~200µs |
| Read (local) | ~10µs | ~10µs | ~10µs |
| Read (primary) | ~100-300µs | ~100-300µs | ~100-300µs |
| Read (quorum) | ~100-200µs | ~100-200µs | ~100-200µs |

### Batch Operations

Use batch APIs for high-throughput distributed writes:

```elixir
# SuperCache batch (routes to primary, single :erpc call per partition)
SuperCache.put_batch!([
  {:user, 1, "Alice"},
  {:user, 2, "Bob"},
  {:user, 3, "Charlie"}
])

# KeyValue batch
KeyValue.add_batch("session", [
  {:user_1, %{name: "Alice"}},
  {:user_2, %{name: "Bob"}}
])
```

Batch operations dramatically reduce network overhead by sending all operations in one message instead of one message per operation.

## Health Monitoring

SuperCache includes a built-in health monitor that continuously tracks cluster health through periodic checks.

### Health Check Functions

```elixir
# Get full cluster health status
SuperCache.Cluster.HealthMonitor.cluster_health()

# Get health status for a specific node
SuperCache.Cluster.HealthMonitor.node_health(node)

# Measure replication lag for a partition
SuperCache.Cluster.HealthMonitor.replication_lag(partition_idx)

# Check partition data balance across nodes
SuperCache.Cluster.HealthMonitor.partition_balance()

# Trigger immediate health check
SuperCache.Cluster.HealthMonitor.force_check()
```

### Health Checks Performed

- **Connectivity** — `:erpc` call to verify node responsiveness and measure RTT
- **Replication** — Write probe key to primary, poll replicas for appearance
- **Partitions** — Compare record counts across nodes to detect imbalance
- **Error rate** — Check Metrics for error counters and failure rates

### Status Levels

| Status | Meaning |
|--------|---------|
| `:healthy` | All checks passing, cluster operating normally |
| `:degraded` | Some checks failing, cluster still functional |
| `:critical` | Major issues detected, data integrity at risk |
| `:unknown` | Unable to determine health (node unreachable) |

### Telemetry Events

Health data is emitted via `:telemetry` events:
- `[:super_cache, :health, :check]` — Periodic health check results
- `[:super_cache, :health, :alert]` — Threshold violations

Wire these to Prometheus/Datadog for real-time dashboards.

## Node Lifecycle

### Adding Nodes

Nodes are added automatically via `:nodeup` events. The cluster manager:
1. Detects the new node
2. Verifies SuperCache is running on it (via health check)
3. Rebuilds the partition map
4. Triggers full sync for owned partitions

You can also manually notify the cluster:

```elixir
SuperCache.Cluster.Manager.node_up(:"new_node@10.0.0.4")
```

### Removing Nodes

Nodes are removed automatically via `:nodedown` events. The cluster manager:
1. Detects the disconnected node
2. Rebuilds the partition map without it
3. Reassigns partitions to remaining nodes

Manual removal:

```elixir
SuperCache.Cluster.Manager.node_down(:"old_node@10.0.0.2")
```

### Full Sync

Force a full partition sync to all peers:

```elixir
SuperCache.Cluster.Manager.full_sync()
```

Useful after network partitions or manual configuration changes.

## NodeMonitor

The `Cluster.NodeMonitor` watches declared nodes and notifies the Manager when they join/leave. It supports three node sources:

| Source | Option | Description |
|--------|--------|-------------|
| Static | `:nodes` | Fixed list evaluated once at startup |
| Dynamic | `:nodes_mfa` | MFA called at init and every `:refresh_ms` |
| Legacy | *(none)* | Watches all Erlang nodes (default) |

### Static Node List

```elixir
config :super_cache, :node_monitor,
  source: :nodes,
  nodes: [:"node1@10.0.0.1", :"node2@10.0.0.2"]
```

### Dynamic Node Discovery (MFA)

```elixir
config :super_cache, :node_monitor,
  source: :nodes_mfa,
  nodes_mfa: {MyApp.NodeDiscovery, :list_nodes, []},
  refresh_ms: 30_000
```

### Reconfigure at Runtime

```elixir
SuperCache.Cluster.NodeMonitor.reconfigure(
  source: :nodes,
  nodes: [:"new_node@10.0.0.3"]
)
```

## Cluster Infrastructure Modules

### Cluster.Manager

Maintains cluster membership and partition → primary/replica mapping. Stores partition map in `:persistent_term` for zero-cost reads.

```elixir
SuperCache.Cluster.Manager.node_up(node)
SuperCache.Cluster.Manager.node_down(node)
SuperCache.Cluster.Manager.full_sync()
SuperCache.Cluster.Manager.get_replicas(partition_idx)
# => {primary_node, [replica_nodes]}
SuperCache.Cluster.Manager.live_nodes()
SuperCache.Cluster.Manager.replication_mode()
```

### Cluster.Router

Routes reads and writes to the correct nodes. Handles read-your-writes consistency, quorum reads, and primary routing.

```elixir
SuperCache.Cluster.Router.route_put!(data, opts \\ [])
SuperCache.Cluster.Router.route_put_batch!(data_list, opts \\ [])
SuperCache.Cluster.Router.route_get!(data, opts \\ [])
SuperCache.Cluster.Router.route_get_by_key_partition!(key, partition_data, opts \\ [])
SuperCache.Cluster.Router.route_get_by_match!(partition_data, pattern, opts \\ [])
SuperCache.Cluster.Router.route_get_by_match_object!(partition_data, pattern, opts \\ [])
SuperCache.Cluster.Router.route_scan!(partition_data, fun, acc)
SuperCache.Cluster.Router.route_delete!(data, opts \\ [])
SuperCache.Cluster.Router.route_delete_all()
SuperCache.Cluster.Router.route_delete_match!(partition_data, pattern)
SuperCache.Cluster.Router.route_delete_by_key_partition!(key, partition_data, opts \\ [])

# Read-your-writes tracking
SuperCache.Cluster.Router.track_write(partition_idx)
SuperCache.Cluster.Router.ryw_recent?(partition_idx)
SuperCache.Cluster.Router.resolve_read_mode(mode, partition_idx)
SuperCache.Cluster.Router.prune_ryw(now)
```

### Cluster.Replicator

Applies replicated writes and handles bulk partition transfers.

```elixir
SuperCache.Cluster.Replicator.replicate(partition_idx, op_name, op_arg \\ nil)
SuperCache.Cluster.Replicator.replicate_batch(partition_idx, op_name, op_args)
SuperCache.Cluster.Replicator.push_partition(partition_idx, target_node)

# erpc entry points (called on replicas)
SuperCache.Cluster.Replicator.apply_op(partition_idx, op_name, op_arg)
SuperCache.Cluster.Replicator.apply_op_batch(partition_idx, op_name, op_args)
```

### Cluster.WAL

Write-Ahead Log for strong consistency. Replaces heavy 3PC with ~200µs latency.

```elixir
SuperCache.Cluster.WAL.commit(partition_idx, ops)
SuperCache.Cluster.WAL.recover()
SuperCache.Cluster.WAL.stats()
# => %{pending: 0, acks_pending: 0, ...}

# erpc entry points
SuperCache.Cluster.WAL.ack(seq, replica_node)
SuperCache.Cluster.WAL.replicate_and_ack(seq, partition_idx, ops)
```

### Cluster.ThreePhaseCommit

Legacy three-phase commit protocol (replaced by WAL for strong consistency). Still available for backwards compatibility.

```elixir
SuperCache.Cluster.ThreePhaseCommit.commit(partition_idx, ops)
SuperCache.Cluster.ThreePhaseCommit.recover()

# Participant callbacks (erpc entry points)
SuperCache.Cluster.ThreePhaseCommit.handle_prepare(txn_id, partition_idx, ops)
SuperCache.Cluster.ThreePhaseCommit.handle_pre_commit(txn_id)
SuperCache.Cluster.ThreePhaseCommit.handle_commit(txn_id, partition_idx, ops)
SuperCache.Cluster.ThreePhaseCommit.handle_abort(txn_id)
```

### Cluster.TxnRegistry

In-memory transaction log for 3PC protocol. Uses `:public` ETS table for lock-free reads.

```elixir
SuperCache.Cluster.TxnRegistry.register(txn_id, partition_idx, ops, replicas)
SuperCache.Cluster.TxnRegistry.mark_pre_committed(txn_id)
SuperCache.Cluster.TxnRegistry.get(txn_id)
SuperCache.Cluster.TxnRegistry.remove(txn_id)
SuperCache.Cluster.TxnRegistry.list_all()
SuperCache.Cluster.TxnRegistry.count()
```

### Cluster.Metrics

Low-overhead counter and latency sample store for observability.

```elixir
SuperCache.Cluster.Metrics.increment(namespace, field)
SuperCache.Cluster.Metrics.get_all(namespace)
SuperCache.Cluster.Metrics.reset(namespace)
SuperCache.Cluster.Metrics.push_latency(key, value_us)
SuperCache.Cluster.Metrics.get_latency_samples(key)
```

### Cluster.Stats

Generates cluster overview, partition maps, and API call statistics.

```elixir
SuperCache.Cluster.Stats.cluster()
SuperCache.Cluster.Stats.partitions()
SuperCache.Cluster.Stats.primary_partitions()
SuperCache.Cluster.Stats.replica_partitions()
SuperCache.Cluster.Stats.node_partitions(target_node)
SuperCache.Cluster.Stats.three_phase_commit()
SuperCache.Cluster.Stats.api()
SuperCache.Cluster.Stats.print(stats)
SuperCache.Cluster.Stats.record(key, %{latency_us: lat, error: err})
SuperCache.Cluster.Stats.record_tpc(event, opts)
```

### Cluster.DistributedStore

Shared routing helpers used by all distributed high-level stores (KeyValue, Queue, Stack, Struct).

```elixir
# Write helpers
SuperCache.Cluster.DistributedStore.route_put(ets_key, value)
SuperCache.Cluster.DistributedStore.route_delete(ets_key, namespace)
SuperCache.Cluster.DistributedStore.route_delete_match(namespace, pattern)

# Read helpers (always local)
SuperCache.Cluster.DistributedStore.local_get(ets_key, namespace)
SuperCache.Cluster.DistributedStore.local_match(namespace, pattern)
SuperCache.Cluster.DistributedStore.local_insert_new(record, namespace)
SuperCache.Cluster.DistributedStore.local_take(ets_key, namespace)
```

## Troubleshooting

### Common Issues

**"Partition count mismatch"**
All nodes must have the same `num_partition` value. Check config on all nodes:
```elixir
SuperCache.Config.get_config(:num_partition)
```

**"Replication lag increasing"**
- Check network connectivity between nodes
- Verify no GC pauses on replicas
- Use `HealthMonitor.cluster_health()` to identify slow nodes
- Consider switching to `:strong` mode for critical data

**"Quorum reads timing out"**
- Ensure majority of nodes are reachable
- Check `:erpc` connectivity: `:erpc.call(:"node@host", :erlang, :node, [], 1000)`
- Verify firewall rules allow distributed Erlang traffic

**"Node not joining cluster"**
- Verify `cluster_peers` config includes all nodes
- Check that all nodes use the same cookie: `Node.get_cookie()`
- Ensure SuperCache is started on all nodes before connecting

### Debugging

Enable debug logging at runtime:

```elixir
SuperCache.Log.enable(true)
```

Or at compile time (zero overhead when disabled):

```elixir
# config/config.exs
config :super_cache, debug_log: true
```

Check cluster statistics:

```elixir
SuperCache.cluster_stats()
# => %{
#   nodes: [:"node1@host", :"node2@host"],
#   partitions: 8,
#   replication_factor: 2,
#   replication_mode: :async
# }
```

Inspect internal state:

```elixir
# Check cluster state
SuperCache.Cluster.Manager.live_nodes()
SuperCache.Cluster.Manager.get_replicas(0)

# Check WAL state
SuperCache.Cluster.WAL.stats()

# Check health metrics
SuperCache.Cluster.HealthMonitor.cluster_health()
SuperCache.Cluster.HealthMonitor.partition_balance()
```

## Best Practices

1. **Match partition counts** — Always use identical `num_partition` across all nodes
2. **Use batch operations** — `put_batch!/1` and `add_batch/2` reduce network overhead by 10-100x
3. **Choose the right replication mode** — `:async` for caches, `:sync` for balanced, `:strong` for critical data
4. **Prefer local reads** — Use `read_mode: :local` when eventual consistency is acceptable
5. **Monitor health metrics** — Wire `:telemetry` events to Prometheus/Datadog for real-time dashboards
6. **Test failure scenarios** — Simulate node crashes and network partitions to verify recovery
7. **Tune WAL timeouts** — Adjust `majority_timeout` based on your network latency
8. **Use NodeMonitor dynamic discovery** — For cloud environments, use `:nodes_mfa` with your service discovery system

## API Reference

### Cluster Management

- `SuperCache.Cluster.Manager.node_up/1` — Manually add a node
- `SuperCache.Cluster.Manager.node_down/1` — Manually remove a node
- `SuperCache.Cluster.Manager.full_sync/0` — Force full partition sync
- `SuperCache.Cluster.Manager.live_nodes/0` — List live cluster nodes
- `SuperCache.Cluster.Manager.replication_mode/0` — Get current replication mode
- `SuperCache.Cluster.Manager.get_replicas/1` — Get `{primary, replicas}` for partition

### Bootstrap

- `SuperCache.Cluster.Bootstrap.start!/1` — Manual cluster bootstrap
- `SuperCache.Cluster.Bootstrap.running?/0` — Check if bootstrap is active
- `SuperCache.Cluster.Bootstrap.stop/0` — Graceful shutdown
- `SuperCache.Cluster.Bootstrap.export_config/0` — Export current config for peer verification

### Health & Metrics

- `SuperCache.Cluster.HealthMonitor.cluster_health/0` — Full cluster health status
- `SuperCache.Cluster.HealthMonitor.node_health/1` — Health for specific node
- `SuperCache.Cluster.HealthMonitor.replication_lag/1` — Replication lag for partition
- `SuperCache.Cluster.HealthMonitor.partition_balance/0` — Partition balance stats
- `SuperCache.Cluster.HealthMonitor.force_check/0` — Trigger immediate health check
- `SuperCache.cluster_stats/0` — Cluster-wide statistics

### NodeMonitor

- `SuperCache.Cluster.NodeMonitor.start_link/1` — Start NodeMonitor
- `SuperCache.Cluster.NodeMonitor.reconfigure/1` — Reconfigure node source at runtime

### Replicator

- `SuperCache.Cluster.Replicator.replicate/3` — Replicate operation to replicas
- `SuperCache.Cluster.Replicator.replicate_batch/3` — Batch replication
- `SuperCache.Cluster.Replicator.push_partition/2` — Sync partition to target node

### WAL

- `SuperCache.Cluster.WAL.commit/2` — Commit via WAL (strong mode)
- `SuperCache.Cluster.WAL.recover/0` — Recover uncommitted entries
- `SuperCache.Cluster.WAL.stats/0` — WAL statistics

### Router

- `SuperCache.Cluster.Router.route_put!/2` — Route write to primary
- `SuperCache.Cluster.Router.route_get!/2` — Route read
- `SuperCache.Cluster.Router.track_write/1` — Track write for read-your-writes
- `SuperCache.Cluster.Router.ryw_recent?/1` — Check if recent write exists
- `SuperCache.Cluster.Router.resolve_read_mode/2` — Resolve effective read mode

### ThreePhaseCommit (Legacy)

- `SuperCache.Cluster.ThreePhaseCommit.commit/2` — Execute 3PC for operations
- `SuperCache.Cluster.ThreePhaseCommit.recover/0` — Recover in-doubt transactions

### TxnRegistry

- `SuperCache.Cluster.TxnRegistry.register/4` — Register new transaction
- `SuperCache.Cluster.TxnRegistry.get/1` — Look up transaction
- `SuperCache.Cluster.TxnRegistry.count/0` — Count in-flight transactions

### Metrics & Stats

- `SuperCache.Cluster.Metrics.increment/2` — Atomically increment counter
- `SuperCache.Cluster.Metrics.get_all/1` — Get all counters as map
- `SuperCache.Cluster.Metrics.push_latency/2` — Push latency sample
- `SuperCache.Cluster.Stats.cluster/0` — Full cluster overview
- `SuperCache.Cluster.Stats.api/0` — Per-operation call counters + latency