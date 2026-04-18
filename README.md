[![Docs](https://img.shields.io/badge/api-docs-green.svg?style=flat)](https://hexdocs.pm/super_cache)
[![Hex.pm](https://img.shields.io/hexpm/v/super_cache.svg?style=flat&color=blue)](https://hex.pm/packages/super_cache)

# SuperCache

## Introduction

High-performance in-memory caching library for Elixir backed by partitioned ETS tables with experimental distributed cluster support. SuperCache provides transparent local and distributed modes with configurable consistency guarantees, batch operations, and multiple data structures.

## Features

- **Partitioned ETS Storage** — Reduces contention by splitting data across multiple ETS tables
- **Multiple Data Structures** — Tuples, key-value namespaces, queues, stacks, and struct storage
- **Distributed Clustering** — Automatic node discovery, partition assignment, and replication
- **Configurable Consistency** — Choose between async, sync (quorum), or strong (WAL) replication
- **Batch Operations** — High-throughput bulk writes with `put_batch!/1`, `add_batch/2`, `remove_batch/2`
- **Performance Optimized** — Compile-time log elimination, partition resolution inlining, worker pools, and early termination quorum reads
- **Health Monitoring** — Built-in cluster health checks with telemetry integration

## Architecture

SuperCache contains **34 modules** organized into 7 layers:

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                          │
│  SuperCache │ KeyValue │ Queue │ Stack │ Struct               │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                    Routing Layer                              │
│  Partition Router (local) │ Cluster Router (distributed)     │
│  Cluster.DistributedStore (shared helpers)                    │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                  Replication Layer                            │
│  Replicator (async/sync) │ WAL (strong) │ ThreePhaseCommit   │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                   Storage Layer                               │
│  Storage (ETS wrapper) │ EtsHolder (table lifecycle)         │
│  Partition (hashing) │ Partition.Holder (registry)           │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                Cluster Infrastructure                         │
│  Manager │ NodeMonitor │ HealthMonitor │ Metrics │ Stats     │
│  TxnRegistry │ Router                                        │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                Buffer System (lazy_put)                       │
│  Buffer (scheduler-affine) → Internal.Queue → Internal.Stream│
└─────────────────────────────────────────────────────────────┘
```

### Module Overview

| Layer | Modules | Responsibility |
|-------|---------|----------------|
| **API** | `SuperCache`, `KeyValue`, `Queue`, `Stack`, `Struct` | Public interfaces for all data structures |
| **Routing** | `Partition`, `Cluster.Router`, `Cluster.DistributedStore`, `Cluster.DistributedHelpers` | Hash-based partition routing, distributed request routing, and shared read/write helpers |
| **Replication** | `Cluster.Replicator`, `Cluster.WAL`, `Cluster.ThreePhaseCommit` | Async/sync/strong replication engines |
| **Storage** | `Storage`, `EtsHolder`, `Partition.Holder` | ETS table management and lifecycle |
| **Cluster** | `Cluster.Manager`, `Cluster.NodeMonitor`, `Cluster.HealthMonitor` | Membership, discovery, and health monitoring |
| **Observability** | `Cluster.Metrics`, `Cluster.Stats`, `Cluster.TxnRegistry` | Counters, latency tracking, and transaction logs |
| **Buffer** | `Buffer`, `Internal.Queue`, `Internal.Stream` | Scheduler-affine write buffers for `lazy_put/1` |

## Installation

**Requirements**: Erlang/OTP 25 or later, Elixir 1.15 or later.

Add `super_cache` to your dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:super_cache, "~> 1.2"}
  ]
end
```

## Quick Start

### Local Mode

```elixir
# Start with defaults (num_partition = schedulers, key_pos = 0, partition_pos = 0)
SuperCache.start!()

# Or with custom config
opts = [key_pos: 0, partition_pos: 1, table_type: :bag, num_partition: 4]
SuperCache.start!(opts)

# Basic tuple operations
SuperCache.put!({:user, 1, "Alice"})
SuperCache.get!({:user, 1})
# => [{:user, 1, "Alice"}]

SuperCache.delete!({:user, 1})
```

### Key-Value API

```elixir
alias SuperCache.KeyValue

KeyValue.add("session", :user_1, %{name: "Alice"})
KeyValue.get("session", :user_1)
# => %{name: "Alice"}

# Batch operations (10-100x faster than individual calls)
KeyValue.add_batch("session", [
  {:user_2, %{name: "Bob"}},
  {:user_3, %{name: "Charlie"}}
])

KeyValue.remove_batch("session", [:user_1, :user_2])
```

### Queue & Stack

```elixir
alias SuperCache.{Queue, Stack}

# FIFO Queue
Queue.add("jobs", "process_order_1")
Queue.add("jobs", "process_order_2")
Queue.out("jobs")
# => "process_order_1"

Queue.peak("jobs")
# => "process_order_2"

# LIFO Stack
Stack.push("history", "page_a")
Stack.push("history", "page_b")
Stack.pop("history")
# => "page_b"
```

### Struct Storage

```elixir
alias SuperCache.Struct

defmodule User do
  defstruct [:id, :name, :email]
end

Struct.init(%User{}, :id)
Struct.add(%User{id: 1, name: "Alice", email: "alice@example.com"})
{:ok, user} = Struct.get(%User{id: 1})
# => {:ok, %User{id: 1, name: "Alice", email: "alice@example.com"}}
```

## Complete API Reference

### SuperCache (Main API)

Primary entry point for tuple storage with transparent local/distributed mode support.

#### Lifecycle
```elixir
SuperCache.start!()
SuperCache.start!(opts)
SuperCache.start()
SuperCache.start(opts)
SuperCache.started?()
SuperCache.stop()
```

#### Write Operations
```elixir
SuperCache.put!(data)
SuperCache.put(data)
SuperCache.lazy_put(data)
SuperCache.put_batch!(data_list)
```

#### Read Operations
```elixir
SuperCache.get!(data, opts \\ [])
SuperCache.get(data, opts \\ [])
SuperCache.get_by_key_partition!(key, partition_data, opts \\ [])
SuperCache.get_same_key_partition!(key, opts \\ [])
SuperCache.get_by_match!(partition_data, pattern, opts \\ [])
SuperCache.get_by_match!(pattern)
SuperCache.get_by_match_object!(partition_data, pattern, opts \\ [])
SuperCache.get_by_match_object!(pattern)
SuperCache.scan!(partition_data, fun, acc)
SuperCache.scan!(fun, acc)
```

#### Delete Operations
```elixir
SuperCache.delete!(data)
SuperCache.delete(data)
SuperCache.delete_all()
SuperCache.delete_by_match!(partition_data, pattern)
SuperCache.delete_by_match!(pattern)
SuperCache.delete_by_key_partition!(key, partition_data)
SuperCache.delete_same_key_partition!(key)
```

#### Partition-Specific Operations
```elixir
SuperCache.put_partition!(data, partition)
SuperCache.get_partition!(key, partition)
SuperCache.delete_partition!(key, partition)
SuperCache.put_partition_by_idx!(data, partition_idx)
SuperCache.get_partition_by_idx!(key, partition_idx)
SuperCache.delete_partition_by_idx!(key, partition_idx)
```

#### Statistics & Mode
```elixir
SuperCache.stats()
SuperCache.cluster_stats()
SuperCache.distributed?()
```

### KeyValue

In-memory key-value namespaces backed by ETS partitions. Multiple independent namespaces coexist using different `kv_name` values.

```elixir
KeyValue.add(kv_name, key, value)
KeyValue.get(kv_name, key, default \\ nil, opts \\ [])
KeyValue.remove(kv_name, key)
KeyValue.remove_all(kv_name)

KeyValue.keys(kv_name, opts \\ [])
KeyValue.values(kv_name, opts \\ [])
KeyValue.count(kv_name, opts \\ [])
KeyValue.to_list(kv_name, opts \\ [])

KeyValue.add_batch(kv_name, pairs)
KeyValue.remove_batch(kv_name, keys)
```

### Queue

Named FIFO queues backed by ETS partitions.

```elixir
Queue.add(queue_name, value)
Queue.out(queue_name, default \\ nil)
Queue.peak(queue_name, default \\ nil, opts \\ [])
Queue.count(queue_name, opts \\ [])
Queue.get_all(queue_name)
```

### Stack

Named LIFO stacks backed by ETS partitions.

```elixir
Stack.push(stack_name, value)
Stack.pop(stack_name, default \\ nil)
Stack.count(stack_name, opts \\ [])
Stack.get_all(stack_name)
```

### Struct

In-memory struct store backed by ETS partitions. Call `init/2` once per struct type before using.

```elixir
Struct.init(struct, key \\ :id)
Struct.add(struct)
Struct.get(struct, opts \\ [])
Struct.get_all(struct, opts \\ [])
Struct.remove(struct)
Struct.remove_all(struct)
```

## Distributed Mode

SuperCache supports distributing data across a cluster of Erlang nodes with configurable consistency guarantees.

### Configuration

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

# config/runtime.exs
config :super_cache,
  cluster_peers: [
    :"node1@10.0.0.1",
    :"node2@10.0.0.2",
    :"node3@10.0.0.3"
  ]
```

### Replication Modes

| Mode | Guarantee | Latency | Use Case |
|------|-----------|---------|----------|
| `:async` | Eventual consistency | ~50-100µs | High-throughput caches, session data |
| `:sync` | Majority ack (adaptive quorum) | ~100-300µs | Balanced durability/performance |
| `:strong` | WAL-based strong consistency | ~200µs | Critical data requiring durability |

**Async Mode**: Fire-and-forget replication via `Task.Supervisor` worker pool. Returns immediately after local write.

**Sync Mode**: Adaptive quorum writes — returns `:ok` once a strict majority of replicas acknowledge, avoiding waits for slow stragglers.

**Strong Mode**: Write-Ahead Log (WAL) replaces heavy 3PC. Writes locally first, then async replicates with majority acknowledgment. ~7x faster than traditional 3PC.

### Read Modes (Distributed)

```elixir
# Local read (fastest, may be stale)
SuperCache.get!({:user, 1})

# Primary read (consistent with primary node)
SuperCache.get!({:user, 1}, read_mode: :primary)

# Quorum read (majority agreement, early termination)
SuperCache.get!({:user, 1}, read_mode: :quorum)
```

**Quorum reads** use early termination — returns as soon as a strict majority agrees, avoiding waits for slow replicas.

### Manual Bootstrap

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

## Performance

### Benchmarks (Local Mode, 4 partitions)

| Operation | Throughput | Notes |
|-----------|-----------|-------|
| `put!` | ~1.2M ops/sec | ~33% overhead vs raw ETS |
| `get!` | ~2.1M ops/sec | Near raw ETS speed |
| `KeyValue.add_batch` (10k) | ~1.1M ops/sec | Single ETS insert |

### Distributed Latency

| Operation | Async | Sync (Quorum) | Strong (WAL) |
|-----------|-------|---------------|--------------|
| Write | ~50-100µs | ~100-300µs | ~200µs |
| Read (local) | ~10µs | ~10µs | ~10µs |
| Read (quorum) | ~100-200µs | ~100-200µs | ~100-200µs |

### Performance Optimizations

1. **Compile-time log elimination** — Debug macros expand to `:ok` when disabled (zero overhead)
2. **Partition resolution inlining** — Single function call with `@compile {:inline}`
3. **Batch ETS operations** — `:ets.insert/2` with lists instead of per-item calls
4. **Async replication worker pool** — `Task.Supervisor` eliminates per-operation `spawn/1` overhead
5. **Adaptive quorum writes** — Returns on majority ack, not all replicas
6. **Quorum read early termination** — Stops waiting once majority is reached
7. **WAL-based strong consistency** — Replaces 3PC with fast local write + async replication + majority ack
8. **Persistent-term config** — Hot-path config keys served from `:persistent_term` for O(1) access
9. **Scheduler-affine buffers** — `lazy_put/1` routes to buffer on same scheduler
10. **Protected ETS tables** — Partition.Holder uses `:protected` ETS for lock-free reads

### WAL Configuration

```elixir
config :super_cache, :wal,
  majority_timeout: 2_000,  # ms to wait for majority ack
  cleanup_interval: 5_000,  # ms between WAL cleanup cycles
  max_pending: 10_000       # max uncommitted entries
```

## Examples

The `examples/` directory contains runnable examples:

- **`examples/local_mode_example.exs`** — Complete local mode demonstration covering all APIs (tuple storage, KeyValue, Queue, Stack, Struct, batch operations)
- **`examples/distributed_mode_example.exs`** — Distributed mode demonstration with cluster configuration, replication modes, read modes, and health monitoring

Run examples with:

```bash
mix run examples/local_mode_example.exs
mix run examples/distributed_mode_example.exs
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `key_pos` | integer | `0` | Tuple index for ETS key lookup |
| `partition_pos` | integer | `0` | Tuple index for partition hashing |
| `num_partition` | integer | schedulers | Number of ETS partitions |
| `table_type` | atom | `:set` | ETS table type (`:set`, `:bag`, `:ordered_set`, `:duplicate_bag`) |
| `table_prefix` | string | `"SuperCache.Storage.Ets"` | Prefix for ETS table atom names |
| `cluster` | atom | `:local` | `:local` or `:distributed` |
| `replication_mode` | atom | `:async` | `:async`, `:sync`, or `:strong` |
| `replication_factor` | integer | `2` | Total copies (primary + replicas) |
| `cluster_peers` | list | `[]` | List of peer node atoms |
| `auto_start` | boolean | `false` | Auto-start on application boot |
| `debug_log` | boolean | `false` | Enable debug logging (compile-time) |

## Health Monitoring

SuperCache includes a built-in health monitor that continuously tracks:

- **Node connectivity** — RTT measurement via `:erpc`
- **Replication lag** — Probe-based delay measurement
- **Partition balance** — Size variance across nodes
- **Operation success rates** — Failed vs total operations

Access health data:

```elixir
SuperCache.Cluster.HealthMonitor.cluster_health()
SuperCache.Cluster.HealthMonitor.node_health(node)
SuperCache.Cluster.HealthMonitor.replication_lag(partition_idx)
SuperCache.Cluster.HealthMonitor.partition_balance()
SuperCache.Cluster.HealthMonitor.force_check()
```

Health data is also emitted via `:telemetry` events:
- `[:super_cache, :health, :check]` — Periodic health check results
- `[:super_cache, :health, :alert]` — Threshold violations

## Debug Logging

Enable at compile time (zero overhead in production):

```elixir
# config/config.exs
config :super_cache, debug_log: true
```

Or toggle at runtime:

```elixir
SuperCache.Log.enable(true)
SuperCache.Log.enable(false)
```

## Troubleshooting

### Common Issues

**"tuple size is lower than key_pos"** — Ensure your tuples have enough elements for the configured `key_pos`.

**"Partition count mismatch"** — All nodes in a cluster must have the same `num_partition` value.

**"Replication lag increasing"** — Check network connectivity between nodes. Use `HealthMonitor.cluster_health()` to diagnose.

**"Quorum reads timing out"** — Ensure majority of nodes are reachable, check `:erpc` connectivity.

### Performance Tips

1. Use `put_batch!/1` for bulk inserts (10-100x faster)
2. Use `KeyValue.add_batch/2` for key-value bulk operations
3. Prefer `:async` replication mode for high-throughput caches
4. Use `read_mode: :local` when eventual consistency is acceptable
5. Enable compile-time `debug_log: false` for production (default)
6. Monitor health metrics and wire telemetry to Prometheus/Datadog

## Guides

- [Usage Guide](guides/Usage.md) — Complete API reference and usage examples
- [Distributed Guide](guides/Distributed.md) — Detailed distributed mode documentation
- [Developer Guide](guides/Developer.md) — Development, testing, benchmarking, and contribution guide

## Testing

```bash
# All tests (includes cluster tests)
mix test

# Unit tests only — no distribution needed
mix test --exclude cluster

# Cluster tests only
mix test.cluster

# Specific test file
mix test test/kv_test.exs

# With warnings as errors
mix test --warnings-as-errors
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Code Style

- Run formatter: `mix format`
- Check for warnings: `mix compile --warnings-as-errors`
- Run tests: `mix test --exclude cluster`

## License

MIT License. See [LICENSE](LICENSE) for details.

## Changelog

### v1.3.0

- **DistributedHelpers module** — Extracted duplicated `apply_write/3`, `route_write/4`, `route_read/5`, `has_partition?/1`, `read_primary/4`, `read_quorum/4` from KeyValue, Queue, Stack, and Struct into a shared `SuperCache.Cluster.DistributedHelpers` module, eliminating ~400 lines of code duplication
- **WAL race condition fix** — Replaced non-atomic `persistent_term` read+write in `next_seq/0` with atomic `:ets.update_counter/4`, preventing duplicate sequence numbers under concurrent commits
- **Config.distributed?/0** — Added a dedicated, inlined `distributed?/0` function to `SuperCache.Config` for zero-cost cluster-mode checks on the hot path (called by every API operation)
- **Queue spin-wait improvement** — Replaced `:timer.sleep(1)` with `:erlang.yield()` in all spin-wait loops, reducing overhead from timer-wheel insertion during lock contention
- **Quorum read early termination** — Standardized all modules to use the KeyValue-style `Task.async` + early-kill approach for quorum reads, which terminates as soon as majority is reached (previously Queue/Stack/Struct used `Task.async_stream` which always waits for all tasks)
- **New test suites** — Added comprehensive tests for `Config` (325 lines), `WAL` (477 lines), `Bootstrap` (554 lines), and `DistributedHelpers` (750 lines), covering lifecycle, validation, concurrent access, edge cases, and race conditions
- **Code formatting** — Consistent formatting across all modified modules

### v1.2.1

- **Unified API** — Local and distributed modes now use the same modules (no separate `Distributed.*` namespaces)
- **Health Monitor** — Added `cluster_health/0`, `node_health/1`, `replication_lag/1`, `partition_balance/0`
- **Read-Your-Writes** — Router tracks recent writes and forces `:primary` reads for consistency
- **NodeMonitor** — Supports static `:nodes`, dynamic `:nodes_mfa`, and legacy all-node watching
- **Buffer System** — Scheduler-affine write buffers for `lazy_put/1` with `Internal.Queue` and `Internal.Stream`
- **Examples** — Added `examples/local_mode_example.exs` and `examples/distributed_mode_example.exs`
- **Documentation** — Complete module reference with all 34 modules documented

### v1.1.0

- **WAL-based strong consistency** — Replaces 3PC with ~7x faster writes (~200µs vs ~1500µs)
- **Adaptive quorum writes** — Sync mode returns on majority ack, not all replicas
- **Replication worker pool** — Eliminates per-operation `spawn/1` overhead
- **Batch API optimizations** — `add_batch/2` uses single ETS insert
- **Quorum read early termination** — Stops waiting once majority is reached
- **Compile-time log elimination** — Zero overhead when debug disabled
- **Partition resolution inlining** — Faster hot-path lookups

### v1.0.0

- Initial release with ETS-backed caching
- Distributed mode with 3PC consistency
- Queue, Stack, KeyValue, and Struct APIs