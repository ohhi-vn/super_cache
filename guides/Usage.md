# SuperCache Guide

SuperCache is a high-performance in-memory caching library for Elixir backed by partitioned ETS tables. It provides a simple API similar to ETS but with added features like partitioning, distributed clustering, batch operations, and multiple data structures.

## Quick Start

### Local Mode

Start SuperCache with default configuration:

```elixir
SuperCache.start!()
```

**Default configuration:**
- `key_pos = 0` — First element of tuple is the ETS key
- `partition_pos = 0` — First element is used for partition hashing
- `table_type = :set` — Standard ETS set (unique keys)
- `num_partition = schedulers` — One partition per online scheduler

Start with custom configuration:

```elixir
opts = [key_pos: 0, partition_pos: 1, table_type: :bag, num_partition: 4]
SuperCache.start!(opts)
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `key_pos` | integer | `0` | Tuple index used as the ETS lookup key |
| `partition_pos` | integer | `0` | Tuple index used to calculate partition |
| `table_type` | atom | `:set` | ETS table type (`:set`, `:bag`, `:ordered_set`) |
| `num_partition` | integer | schedulers | Number of ETS partitions |

> **Note:** In most cases, `key_pos` and `partition_pos` should be the same. If you need to organize data for fast access, you can choose a different element to calculate the partition.

## Basic Usage

### Tuple Storage

```elixir
opts = [key_pos: 0, partition_pos: 1, table_type: :bag, num_partition: 3]
SuperCache.start!(opts)

# Insert a tuple
SuperCache.put!({:hello, :world, "hello world!"})

# Retrieve by key and partition
SuperCache.get_by_key_partition!(:hello, :world)
# => [{:hello, :world, "hello world!"}]

# Delete by key and partition
SuperCache.delete_by_key_partition!(:hello, :world)

# Pattern matching
SuperCache.put!({:a, :b, 1, 3, "c"})
SuperCache.get_by_match_object!({:_, :_, 1, :_, "c"})
# => [{:a, :b, 1, 3, "c"}]
```

### Key-Value API

```elixir
alias SuperCache.KeyValue

# Start cache
SuperCache.start!()

# Basic operations
KeyValue.add("my_kv", :key, "Hello")
KeyValue.get("my_kv", :key)
# => "Hello"

KeyValue.remove("my_kv", :key)
KeyValue.get("my_kv", :key)
# => nil

# Collection operations
KeyValue.keys("my_kv")
KeyValue.values("my_kv")
KeyValue.count("my_kv")
KeyValue.to_list("my_kv")

# Clear namespace
KeyValue.add("my_kv", :key, "Hello")
KeyValue.remove_all("my_kv")
```

### Queue (FIFO)

```elixir
alias SuperCache.Queue

# Start cache
SuperCache.start!()

Queue.add("my_queue", "Hello")
Queue.out("my_queue")
# => "Hello"

# Peak without removing
Queue.add("my_queue", "World")
Queue.peak("my_queue")
# => "World"

# Get all items
Queue.get_all("my_queue")
# => ["Hello", "World"]
```

### Stack (LIFO)

```elixir
alias SuperCache.Stack

# Start cache
SuperCache.start!()

Stack.push("my_stack", "Hello")
Stack.pop("my_stack")
# => "Hello"

# Peak without removing
Stack.push("my_stack", "World")
Stack.peak("my_stack")
# => "World"
```

### Struct Storage

```elixir
alias SuperCache.Struct

defmodule User do
  defstruct [:id, :name, :email]
end

# Start cache
SuperCache.start!()

# Initialize key storage for struct type
Struct.init(%User{}, :id)

# Add struct
user = %User{id: 1, name: "Alice", email: "alice@example.com"}
Struct.add(user)

# Get struct
{:ok, result} = Struct.get(%User{id: 1})
# => %User{id: 1, name: "Alice", email: "alice@example.com"}

# Get all structs
{:ok, users} = Struct.get_all(%User{})

# Remove struct
Struct.remove(%User{id: 1})

# Remove all structs of this type
Struct.remove_all(%User{})
```

## Batch Operations

Batch operations are **10-100x faster** than individual calls because they reduce function call overhead and use single ETS operations.

### SuperCache Batch

```elixir
# Insert multiple tuples at once
SuperCache.put_batch!([
  {:user, 1, "Alice"},
  {:user, 2, "Bob"},
  {:user, 3, "Charlie"}
])
```

### KeyValue Batch

```elixir
alias SuperCache.KeyValue

# Add multiple key-value pairs
KeyValue.add_batch("session", [
  {:user_1, %{name: "Alice"}},
  {:user_2, %{name: "Bob"}},
  {:user_3, %{name: "Charlie"}}
])

# Remove multiple keys
KeyValue.remove_batch("session", [:user_1, :user_2])
```

> **Performance Tip:** Always use batch operations for bulk inserts. `add_batch/2` uses a single `:ets.insert/2` call with a list, which is dramatically faster than calling `add/3` in a loop.

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

### Read Modes

```elixir
# Local read (fastest, may be stale)
SuperCache.get!({:user, 1})

# Primary read (consistent with primary node)
SuperCache.get!({:user, 1}, read_mode: :primary)

# Quorum read (majority agreement with early termination)
SuperCache.get!({:user, 1}, read_mode: :quorum)
```

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

## Performance Tips

### 1. Use Batch Operations

Always prefer batch APIs for bulk operations:

```elixir
# ❌ Slow: N individual ETS calls
Enum.each(1..10_000, fn i ->
  SuperCache.put!({:user, i, "data_#{i}"})
end)

# ✅ Fast: Single ETS call
SuperCache.put_batch!(Enum.map(1..10_000, fn i ->
  {:user, i, "data_#{i}"}
end))
```

### 2. Choose the Right Replication Mode

- **`:async`** — Best for caches where occasional data loss is acceptable
- **`:sync`** — Good balance of durability and performance
- **`:strong`** — Use for critical data that must survive node crashes

### 3. Prefer Local Reads

Use `read_mode: :local` when eventual consistency is acceptable. It's ~10x faster than primary or quorum reads.

### 4. Tune Partition Count

- More partitions = less contention but more memory overhead
- Start with `num_partition = schedulers` (default)
- Increase if you see contention under high write throughput

### 5. Enable Compile-Time Log Elimination

Debug logging is disabled by default. Ensure it stays disabled in production:

```elixir
# config/config.exs
config :super_cache, debug_log: false  # Zero overhead when disabled
```

### 6. Monitor Health Metrics

SuperCache includes a built-in health monitor:

```elixir
# Get current health status
SuperCache.Cluster.HealthMonitor.health()

# Get detailed metrics
SuperCache.Cluster.HealthMonitor.metrics()

# Get cluster statistics
SuperCache.cluster_stats()
```

## Performance Benchmarks

### Local Mode (4 partitions)

| Operation | Throughput | Notes |
|-----------|-----------|-------|
| `put!` | ~1.2M ops/sec | ~33% overhead vs raw ETS |
| `get!` | ~2.1M ops/sec | Near raw ETS speed |
| `KeyValue.add_batch` (10k) | ~1.1M ops/sec | Single ETS insert |

### Distributed Mode (LAN)

| Operation | Async | Sync (Quorum) | Strong (WAL) |
|-----------|-------|---------------|--------------|
| Write | ~50-100µs | ~100-300µs | ~200µs |
| Read (local) | ~10µs | ~10µs | ~10µs |
| Read (quorum) | ~100-200µs | ~100-200µs | ~100-200µs |

## Troubleshooting

### Common Issues

**"tuple size is lower than key_pos"**
Ensure your tuples have enough elements for the configured `key_pos`.

**"Partition count mismatch"**
All nodes in a cluster must have the same `num_partition` value.

**"Replication lag increasing"**
- Check network connectivity between nodes
- Verify no GC pauses on replicas
- Use `HealthMonitor.metrics()` to identify slow nodes

### Debug Logging

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

## Architecture

```
Client → API → Partition Router → Storage (ETS)
                ↓
        Distributed Router (optional)
                ↓
        Replicator → Remote Nodes
```

### Components

1. **API Layer** — Public interface (`SuperCache`, `KeyValue`, `Queue`, `Stack`, `Struct`)
2. **Partition Layer** — Hash-based routing to ETS tables (`Partition`, `Partition.Holder`)
3. **Storage Layer** — ETS table management (`Storage`, `EtsHolder`)
4. **Cluster Layer** — Distributed coordination (`Manager`, `Replicator`, `WAL`, `Router`)

## API Reference

### SuperCache (Main API)

- `start!/1`, `start/1` — Start cache with options
- `put!/1`, `put/1` — Insert tuple
- `put_batch!/1` — Batch insert (10-100x faster)
- `get!/2`, `get/2` — Retrieve by key
- `delete!/1`, `delete/1` — Remove by key
- `delete_all/0` — Clear all partitions
- `get_by_match!/3`, `get_by_match_object!/3` — Pattern matching
- `scan!/3` — Fold over partition records
- `stats/0` — Get cache statistics
- `distributed?/0` — Check if running in distributed mode

### KeyValue

- `add/3`, `get/4`, `remove/2` — Basic operations
- `add_batch/2`, `remove_batch/2` — Batch operations
- `keys/2`, `values/2`, `count/2`, `to_list/2` — Collection operations
- `remove_all/1` — Clear namespace

### Queue

- `add/2`, `out/1`, `peak/1` — FIFO operations
- `count/1`, `get_all/1` — Inspection

### Stack

- `push/2`, `pop/1`, `peak/1` — LIFO operations
- `count/1`, `get_all/1` — Inspection

### Struct

- `init/2` — Initialize struct type with key field
- `add/1`, `get/2`, `remove/1` — CRUD operations
- `get_all/2`, `remove_all/1` — Bulk operations

## Further Reading

- [Distributed Cache Guide](Distributed.md) — Detailed distributed mode documentation
- [DEV_GUIDE.md](DEV_GUIDE.md) — Development and contribution guide
- [HexDocs](https://hexdocs.pm/super_cache) — Full API documentation