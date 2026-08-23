---
name: super-cache-distributed
description: Use when an application depending on super_cache must run as a multi-node cluster — enabling distributed mode, choosing replication modes (:async/:sync/:strong), read consistency (local/primary/quorum), node join/leave behaviour, health monitoring, and cluster troubleshooting. Load this before configuring or debugging SuperCache across nodes.
---

# SuperCache — Distributed Mode

Turns the single-node ETS cache into a partitioned, replicated cluster cache.
All nodes MUST run identical structural config (`num_partition`, `table_type`,
`replication_factor`, `replication_mode`, `key_pos`, `partition_pos`) —
`Bootstrap.start!/1` verifies this against peers at join time and rejects
mismatches with an `ArgumentError` naming the disagreeing peer.

## Enabling distributed mode

```elixir
SuperCache.Cluster.Bootstrap.start!(
  key_pos: 0,
  partition_pos: 0,
  num_partition: 8,            # identical on every node
  cluster: :distributed,
  replication_factor: 2,       # primary + 1 replica per partition
  replication_mode: :async     # :async | :sync | :strong
)
```

Connect peers yourself (or via libcluster/your discovery):

```elixir
true = Node.connect(:"node2@10.0.0.2")
```

Membership is picked up automatically from Erlang `:nodeup`/`:nodedown`
events. For service-discovery integration use a dynamic source:

```elixir
SuperCache.Cluster.NodeMonitor.reconfigure(
  nodes_mfa: {MyApp.Discovery, :cache_nodes, []},
  refresh_ms: 30_000
)

SuperCache.Cluster.NodeMonitor.reconfigure(nodes: [:"node1@...", :"node2@..."])  # static
```

## Replication modes

| Mode | Guarantee | Choose when |
|------|-----------|-------------|
| `:async` | Eventual; fire-and-forget via supervised worker pool | Caches, sessions — default |
| `:sync`  | Returns after majority ack (adaptive quorum)          | Durable-but-fast |
| `:strong`| WAL-based; local write + async replicate + majority ack (~200µs) | Must survive primary crash |

```elixir
# WAL tuning (only used by :strong):
config :super_cache, :wal,
  majority_timeout: 2_000,
  cleanup_interval: 5_000
```

## Read consistency

```elixir
SuperCache.get!(data)                            # local ETS — fastest, maybe stale
SuperCache.get!(data, read_mode: :primary)       # routed to partition primary
SuperCache.get!(data, read_mode: :quorum)        # majority agreement across holders
```

Read-your-writes: after `put!`, reads of the same partition automatically
upgrade to primary for ~5s, so you see your own writes even in local mode.

## Node lifecycle

- **Join**: detected via `:nodeup`; manager health-checks the node, rebuilds
  the partition map, pushes owned partitions to the joiner, and *reconciles
  ownership* — existing nodes that gained partitions pull their data from the
  previous holders.
- **Leave**: detected via `:nodedown`; map rebuilds without the node and
  survivors pull partitions they inherit (best-effort from remaining holders).
- Manual control:

```elixir
SuperCache.Cluster.Manager.node_up(:"new@host")
SuperCache.Cluster.Manager.node_down(:"dead@host")
SuperCache.Cluster.Manager.full_sync()      # push all owned partitions to peers
SuperCache.Cluster.Manager.live_nodes()
SuperCache.Cluster.Manager.get_replicas(idx) # => {primary, [replicas]}
```

## Health & observability

```elixir
h = SuperCache.Cluster.HealthMonitor.cluster_health()
# %{status: :healthy|:degraded|:unhealthy|:unknown, nodes: [...], summary: %{}}

HealthMonitor.node_health(node())
HealthMonitor.replication_lag(partition_idx)
HealthMonitor.partition_balance()
HealthMonitor.force_check()

SuperCache.cluster_stats()
SuperCache.Cluster.WAL.stats()               # %{pending: _, acks_pending: _}
```

Optional telemetry events (emitted only when the `:telemetry` dependency is
loaded): `[:super_cache, :health, :check]` and `[:super_cache, :health, :alert]`.

## Troubleshooting quick reference

| Symptom | First checks |
|---|---|
| Join raises config-mismatch `ArgumentError` | Compare `Config.get_config(:num_partition)` etc. on both nodes |
| Node never joins | Same cookie? `Node.connect/1` true? Bootstrap running on the peer? |
| Replica misses writes | Check `cluster_health()`; async mode tolerates loss — switch to `:sync` |
| Quorum read returns stale/empty | Majority of holders unreachable — inspect `Manager.get_replicas(idx)` |
| After crash of one node | Survivors auto-rebalance; verify with `partition_balance()` |

## Testing your integration

Run against a real two-node setup (or reuse the library's own pattern:
`:peer` nodes + `ClusterCase`). The library's suite does exactly this via
`mix test.cluster`.
