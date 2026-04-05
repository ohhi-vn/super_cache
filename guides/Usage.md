# SuperCache Usage Guide

SuperCache is a high-performance in-memory caching library for Elixir backed by partitioned ETS tables. It provides transparent local and distributed modes with configurable consistency guarantees, batch operations, and multiple data structures.

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
| `table_type` | atom | `:set` | ETS table type (`:set`, `:bag`, `:ordered_set`, `:duplicate_bag`) |
| `num_partition` | integer | schedulers | Number of ETS partitions |
| `table_prefix` | string | `"SuperCache.Storage.Ets"` | Prefix for ETS table atom names |
| `cluster` | atom | `:local` | `:local` or `:distributed` |
| `replication_factor` | integer | `2` | Total copies (primary + replicas) |
| `replication_mode` | atom | `:async` | `:async`, `:sync`, or `:strong` |
| `auto_start` | boolean | `false` | Auto-start on application boot |

> **Note:** In most cases, `key_pos` and `partition_pos` should be the same. If you need to organize data for fast access, you can choose a different element to calculate the partition.

## Complete API Reference

### SuperCache (Main API)

Primary entry point for all cache operations. Handles both local and distributed modes transparently.

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
SuperCache.get_by_key_partition(key, partition_data, opts \\ [])
SuperCache.get_same_key_partition!(key, opts \\ [])
SuperCache.get_same_key_partition(key, opts \\ [])
SuperCache.get_by_match!(partition_data, pattern, opts \\ [])
SuperCache.get_by_match!(pattern)
SuperCache.get_by_match(partition_data, pattern, opts \\ [])
SuperCache.get_by_match_object!(partition_data, pattern, opts \\ [])
SuperCache.get_by_match_object!(pattern)
SuperCache.get_by_match_object(partition_data, pattern, opts \\ [])
SuperCache.scan!(partition_data, fun, acc)
SuperCache.scan!(fun, acc)
SuperCache.scan(partition_data, fun, acc)
```

#### Delete Operations

```elixir
SuperCache.delete!(data)
SuperCache.delete(data)
SuperCache.delete_all()
SuperCache.delete_by_match!(partition_data, pattern)
SuperCache.delete_by_match!(pattern)
SuperCache.delete_by_match(partition_data, pattern)
SuperCache.delete_by_key_partition!(key, partition_data)
SuperCache.delete_by_key_partition(key, partition_data)
SuperCache.delete_same_key_partition!(key)
SuperCache.delete_same_key_partition(key)
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

#### Read Modes (Distributed)

Pass `read_mode` option to read functions:
- `:local` (default) — Fastest, may be stale
- `:primary` — Routes to partition's primary node
- `:quorum` — Majority agreement with early termination

```elixir
SuperCache.get!({:user, 1})
SuperCache.get!({:user, 1}, read_mode: :primary)
SuperCache.get!({:user, 1}, read_mode: :quorum)
```

---

### KeyValue

In-memory key-value namespaces backed by ETS partitions. Multiple independent namespaces coexist using different `kv_name` values.

```elixir
alias SuperCache.KeyValue
```

#### Basic Operations

```elixir
KeyValue.add(kv_name, key, value)
KeyValue.get(kv_name, key, default \\ nil, opts \\ [])
KeyValue.remove(kv_name, key)
KeyValue.remove_all(kv_name)
```

#### Collection Operations

```elixir
KeyValue.keys(kv_name, opts \\ [])
KeyValue.values(kv_name, opts \\ [])
KeyValue.count(kv_name, opts \\ [])
KeyValue.to_list(kv_name, opts \\ [])
```

#### Batch Operations

```elixir
KeyValue.add_batch(kv_name, pairs)
KeyValue.remove_batch(kv_name, keys)
```

#### Example

```elixir
KeyValue.add("session", :user_1, %{name: "Alice"})
KeyValue.get("session", :user_1)
# => %{name: "Alice"}

KeyValue.add_batch("session", [
  {:user_2, %{name: "Bob"}},
  {:user_3, %{name: "Charlie"}}
])

KeyValue.keys("session")
# => [:user_1, :user_2, :user_3]

KeyValue.remove("session", :user_1)
KeyValue.remove_all("session")
```

---

### Queue

Named FIFO queues backed by ETS partitions.

```elixir
alias SuperCache.Queue
```

#### Operations

```elixir
Queue.add(queue_name, value)
Queue.out(queue_name, default \\ nil)
Queue.peak(queue_name, default \\ nil, opts \\ [])
Queue.count(queue_name, opts \\ [])
Queue.get_all(queue_name)
```

#### Example

```elixir
Queue.add("jobs", "process_order_1")
Queue.add("jobs", "process_order_2")

Queue.peak("jobs")
# => "process_order_1"

Queue.out("jobs")
# => "process_order_1"

Queue.count("jobs")
# => 1

Queue.get_all("jobs")
# => ["process_order_2"]
```

---

### Stack

Named LIFO stacks backed by ETS partitions.

```elixir
alias SuperCache.Stack
```

#### Operations

```elixir
Stack.push(stack_name, value)
Stack.pop(stack_name, default \\ nil)
Stack.count(stack_name, opts \\ [])
Stack.get_all(stack_name)
```

#### Example

```elixir
Stack.push("history", "page_a")
Stack.push("history", "page_b")

Stack.pop("history")
# => "page_b"

Stack.count("history")
# => 1

Stack.get_all("history")
# => ["page_a"]
```

---

### Struct

In-memory struct store backed by ETS partitions. Call `init/2` once per struct type before using.

```elixir
alias SuperCache.Struct
```

#### Operations

```elixir
Struct.init(struct, key \\ :id)
Struct.add(struct)
Struct.get(struct, opts \\ [])
Struct.get_all(struct, opts \\ [])
Struct.remove(struct)
Struct.remove_all(struct)
```

#### Example

```elixir
defmodule User do
  defstruct [:id, :name, :email]
end

Struct.init(%User{}, :id)

Struct.add(%User{id: 1, name: "Alice", email: "alice@example.com"})
Struct.add(%User{id: 2, name: "Bob", email: "bob@example.com"})

{:ok, user} = Struct.get(%User{id: 1})
# => {:ok, %User{id: 1, name: "Alice", email: "alice@example.com"}}

{:ok, users} = Struct.get_all(%User{})
# => {:ok, [%User{...}, %User{...}]}

Struct.remove(%User{id: 1})
Struct.remove_all(%User{})
```

---

## Batch Operations

Batch operations are **10-100x faster** than individual calls because they reduce function call overhead and use single ETS operations.

### SuperCache Batch

```elixir
SuperCache.put_batch!([
  {:user, 1, "Alice"},
  {:user, 2, "Bob"},
  {:user, 3, "Charlie"}
])
```

### KeyValue Batch

```elixir
KeyValue.add_batch("session", [
  {:user_1, %{name: "Alice"}},
  {:user_2, %{name: "Bob"}}
])

KeyValue.remove_batch("session", [:user_1, :user_2])
```

> **Performance Tip:** Always use batch operations for bulk inserts. `add_batch/2` uses a single `:ets.insert/2` call with a list, which is dramatically faster than calling `add/3` in a loop.

---

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

---

## Performance Tips

### 1. Use Batch Operations

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

```elixir
SuperCache.Cluster.HealthMonitor.cluster_health()
SuperCache.Cluster.HealthMonitor.node_health(node)
SuperCache.cluster_stats()
```

---

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

---

## Troubleshooting

### Common Issues

**"tuple size is lower than key_pos"**
Ensure your tuples have enough elements for the configured `key_pos`.

**"Partition count mismatch"**
All nodes in a cluster must have the same `num_partition` value.

**"Replication lag increasing"**
- Check network connectivity between nodes
- Verify no GC pauses on replicas
- Use `HealthMonitor.cluster_health()` to identify slow nodes

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

---

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

---

## Further Reading

- [Distributed Cache Guide](Distributed.md) — Detailed distributed mode documentation
- [Developer Guide](Developer.md) — Development and contribution guide
- [HexDocs](https://hexdocs.pm/super_cache) — Full API documentation