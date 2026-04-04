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

SuperCache includes a built-in health monitor that continuously tracks:

- **Node connectivity** — RTT measurement via `:erpc`
- **Replication lag** — Probe-based delay measurement
- **Partition balance** — Size variance across nodes
- **Operation success rates** — Failed vs total operations

Access health data:

```elixir
# Get current health status
SuperCache.Cluster.HealthMonitor.health()

# Get detailed metrics
SuperCache.Cluster.HealthMonitor.metrics()

# Get cluster statistics
SuperCache.cluster_stats()
```

Health data is also emitted via `:telemetry` events:
- `[:super_cache, :health, :check]` — Periodic health check results
- `[:super_cache, :health, :alert]` — Threshold violations

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
- Use `HealthMonitor.metrics()` to identify slow nodes
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

## Best Practices

1. **Match partition counts** — Always use identical `num_partition` across all nodes
2. **Use batch operations** — `put_batch!/1` and `add_batch/2` reduce network overhead by 10-100x
3. **Choose the right replication mode** — `:async` for caches, `:sync` for balanced, `:strong` for critical data
4. **Prefer local reads** — Use `read_mode: :local` when eventual consistency is acceptable
5. **Monitor health metrics** — Wire `:telemetry` events to Prometheus/Datadog for real-time dashboards
6. **Test failure scenarios** — Simulate node crashes and network partitions to verify recovery
7. **Tune WAL timeouts** — Adjust `majority_timeout` based on your network latency

## API Reference

### Cluster Management

- `SuperCache.Cluster.Manager.node_up/1` — Manually add a node
- `SuperCache.Cluster.Manager.node_down/1` — Manually remove a node
- `SuperCache.Cluster.Manager.full_sync/0` — Force full partition sync
- `SuperCache.Cluster.Manager.live_nodes/0` — List live cluster nodes
- `SuperCache.Cluster.Manager.replication_mode/0` — Get current replication mode

### Bootstrap

- `SuperCache.Cluster.Bootstrap.start!/1` — Manual cluster bootstrap
- `SuperCache.Cluster.Bootstrap.running?/0` — Check if bootstrap is active
- `SuperCache.Cluster.Bootstrap.stop/0` — Graceful shutdown

### Health & Metrics

- `SuperCache.Cluster.HealthMonitor.health/0` — Current health status
- `SuperCache.Cluster.HealthMonitor.metrics/0` — Detailed metrics
- `SuperCache.Cluster.HealthMonitor.stats/0` — Health statistics
- `SuperCache.cluster_stats/0` — Cluster-wide statistics

### Replicator

- `SuperCache.Cluster.Replicator.replicate/3` — Replicate operation to replicas
- `SuperCache.Cluster.Replicator.replicate_batch/3` — Batch replication
- `SuperCache.Cluster.Replicator.push_partition/2` — Sync partition to target node

### WAL

- `SuperCache.Cluster.WAL.commit/2` — Commit via WAL (strong mode)
- `SuperCache.Cluster.WAL.recover/0` — Recover uncommitted entries
- `SuperCache.Cluster.WAL.stats/0` — WAL statistics