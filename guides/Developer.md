# Developer Guide for SuperCache

Clone [repo](https://github.com/ohhi-vn/super_cache)

## Setup

```bash
mix deps.get
```

For using Tidewave (live reload during development):

```bash
mix tidewave
```

Go to [http://localhost:4000/tidewave](http://localhost:4000/tidewave)

## Using as Local Dependency

In your project's `mix.exs`:

```elixir
defp deps do
  base_dir = "/your/base/path"

  [
    {:super_cache, path: Path.join(base_dir, "super_cache")}
    
    # or using git repo, using override: true if you want to override in sub deps.
    # {:super_cache, git: "https://github.com/your_account/super_cache", override: true}
  ]
end
```

## Architecture Overview

SuperCache is built on a layered architecture designed for high performance and horizontal scalability:

```
┌─────────────────────────────────────────────────────────────┐
│                        Client Layer                          │
│  SuperCache | KeyValue | Queue | Stack | Struct              │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                     Routing Layer                            │
│  Partition Router (local) | Cluster Router (distributed)    │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                   Replication Layer                          │
│  Replicator (async/sync) | WAL (strong)                     │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                    Storage Layer                             │
│  Storage (ETS wrapper) | EtsHolder (lifecycle)              │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

| Component | Path | Responsibility |
|-----------|------|----------------|
| `SuperCache` | `lib/api/super_cache.ex` | Main public API for tuple storage |
| `KeyValue` | `lib/api/key_value.ex` | Key-value namespace API with batch operations |
| `Queue` / `Stack` | `lib/api/queue.ex`, `lib/api/stack.ex` | FIFO/LIFO data structures |
| `Struct` | `lib/api/struct.ex` | Struct storage with automatic key extraction |
| `Partition` | `lib/partition/partition_api.ex` | Hash-based partition routing |
| `Storage` | `lib/storage/storage_api.ex` | Thin ETS wrapper with concurrency optimizations |
| `Cluster.Manager` | `lib/cluster/manager.ex` | Cluster membership and partition assignment |
| `Cluster.Replicator` | `lib/cluster/replicator.ex` | Replication with worker pool and adaptive quorum |
| `Cluster.WAL` | `lib/cluster/wal.ex` | Write-Ahead Log for strong consistency |
| `Cluster.Router` | `lib/cluster/router.ex` | Distributed read/write routing with quorum reads |
| `Cluster.HealthMonitor` | `lib/cluster/health_monitor.ex` | Continuous health checking and telemetry |

### Performance Optimizations

The codebase includes several performance optimizations:

1. **Compile-time log elimination** — `SuperCache.Log.debug/1` expands to `:ok` when `debug_log: false`
2. **Partition resolution inlining** — `@compile {:inline, get_partition: 1}` eliminates function call overhead
3. **Batch ETS operations** — `:ets.insert/2` with lists instead of per-item calls
4. **Async replication worker pool** — `Task.Supervisor` eliminates per-operation `spawn/1` overhead
5. **Adaptive quorum writes** — Sync mode returns on majority ack, not all replicas
6. **Quorum read early termination** — Stops waiting once majority is reached
7. **WAL-based strong consistency** — Replaces 3PC with fast local write + async replication + majority ack

## Usage Guide in Plain IEx (Single Cluster, Two Nodes)

### Start Cluster

Terminal 1:
```bash
iex --name node1@127.0.0.1 --cookie need_to_change_this -S mix
```

Terminal 2:
```bash
iex --name node2@127.0.0.1 --cookie need_to_change_this -S mix
```

### Bootstrap and Test

```elixir
# --- node1@127.0.0.1 ---

# Connect to peer
Node.connect(:"node2@127.0.0.1")

# Start cache (if :auto_start is missed or false)
SuperCache.Cluster.Bootstrap.start!(
  key_pos: 0, partition_pos: 0,
  cluster: :distributed,
  replication_mode: :strong,  # Try :async, :sync, or :strong
  replication_factor: 2,
  num_partition: 4
)

# Verify partition map is built
SuperCache.Cluster.Manager.live_nodes()
#=> [:"node1@127.0.0.1", :"node2@127.0.0.1"]

# Check cluster stats
SuperCache.cluster_stats()

# Write data
SuperCache.put!({:user, 1, "Alice"})
SuperCache.put!({:user, 2, "Bob"})

# Check record counts per partition
SuperCache.stats()
```

```elixir
# --- node2@127.0.0.1 ---

# Verify node1 is joined
Node.list()
#=> [:"node1@127.0.0.1"]

# Start cache (must match node1 config)
SuperCache.Cluster.Bootstrap.start!(
  key_pos: 0, partition_pos: 0,
  cluster: :distributed,
  replication_mode: :strong,
  replication_factor: 2,
  num_partition: 4
)

# Read replicated data
SuperCache.get!({:user, 1})
#=> [{:user, 1, "Alice"}]

# Read with different consistency levels
SuperCache.get!({:user, 1}, read_mode: :local)    # Fast, may be stale
SuperCache.get!({:user, 1}, read_mode: :primary)  # Consistent with primary
SuperCache.get!({:user, 1}, read_mode: :quorum)   # Majority agreement
```

## Testing

### Run All Tests

```bash
# All tests (includes cluster tests)
mix test

# Unit tests only — no distribution needed
mix test --exclude cluster

# Specific test file
mix test test/kv_test.exs

# Specific test with line number
mix test test/kv_test.exs:42

# With warnings as errors
mix test --warnings-as-errors
```

### Cluster Tests

```bash
# Run only cluster tests
mix test test/cluster/

# Run with specific seed for reproducibility
mix test test/cluster/ --seed 12345
```

> **Note:** Cluster tests can be flaky due to timing and shared node config. Use `--exclude cluster` for fast local development.

### Testing WAL Consistency

```elixir
# Start with strong mode
SuperCache.Cluster.Bootstrap.start!(
  key_pos: 0, partition_pos: 0,
  cluster: :distributed,
  replication_mode: :strong,
  replication_factor: 2,
  num_partition: 4
)

# Write data
SuperCache.put!({:wal_test, 1, "data"})

# Check WAL stats
SuperCache.Cluster.WAL.stats()
#=> %{pending: 0, acks_pending: 0}

# Test recovery (simulate restart)
SuperCache.Cluster.WAL.recover()
```

### Testing Replication Modes

```elixir
# Test async mode (fire-and-forget)
SuperCache.Cluster.Bootstrap.start!(
  key_pos: 0, partition_pos: 0,
  cluster: :distributed,
  replication_mode: :async,
  replication_factor: 2,
  num_partition: 4
)

# Test sync mode (adaptive quorum)
SuperCache.Cluster.Bootstrap.start!(
  key_pos: 0, partition_pos: 0,
  cluster: :distributed,
  replication_mode: :sync,
  replication_factor: 2,
  num_partition: 4
)
```

## Benchmark & Profiling

### Built-in Benchmark Tools

Scripts are located in `./tools/`:

```bash
# Performance benchmark
mix run tools/performance.exs

# Profiling with fprof
mix run tools/profile.exs
```

### Quick Benchmarks in IEx

```elixir
# Start cache
SuperCache.start!(key_pos: 0, partition_pos: 1, num_partition: 4)

# Benchmark put!
n = 100_000
{time_us, _} = :timer.tc(fn ->
  Enum.each(1..n, fn i -> SuperCache.put!({:user, i, "data"}) end)
end)
ops_per_sec = n / (time_us / 1_000_000)
IO.puts("put! #{n} ops in #{time_us}µs = #{round(ops_per_sec)} ops/sec")

# Benchmark get!
{time_us, _} = :timer.tc(fn ->
  Enum.each(1..n, fn i -> SuperCache.get!({:user, i}) end)
end)
ops_per_sec = n / (time_us / 1_000_000)
IO.puts("get! #{n} ops in #{time_us}µs = #{round(ops_per_sec)} ops/sec")

# Benchmark batch operations
alias SuperCache.KeyValue
{time_us, _} = :timer.tc(fn ->
  KeyValue.add_batch("bench", Enum.map(1..10_000, fn i -> {i, "val"} end))
end)
ops_per_sec = 10_000 / (time_us / 1_000_000)
IO.puts("KeyValue.add_batch 10k in #{time_us}µs = #{round(ops_per_sec)} ops/sec")

SuperCache.stop()
```

### Expected Performance (Local Mode, 4 partitions)

| Operation | Throughput | Notes |
|-----------|-----------|-------|
| `put!` | ~1.2M ops/sec | ~33% overhead vs raw ETS |
| `get!` | ~2.1M ops/sec | Near raw ETS speed |
| `KeyValue.add_batch` (10k) | ~1.1M ops/sec | Single ETS insert |

### Distributed Latency (Typical LAN)

| Operation | Async | Sync (Quorum) | Strong (WAL) |
|-----------|-------|---------------|--------------|
| Write | ~50-100µs | ~100-300µs | ~200µs |
| Read (local) | ~10µs | ~10µs | ~10µs |
| Read (quorum) | ~100-200µs | ~100-200µs | ~100-200µs |

### Profiling with :fprof

```elixir
# Start profiling
:fprof.apply(SuperCache, :put!, [{:user, 1, "Alice"}])
:fprof.profile()
:fprof.analyse()
```

### Memory Profiling

```elixir
# Check ETS table sizes
:ets.all()
|> Enum.map(fn table -> {table, :ets.info(table, :size)} end)
|> Enum.sort_by(fn {_, size} -> size end, :desc)
|> Enum.take(10)
```

## Debugging

### Enable Debug Logging

Compile-time (zero overhead when disabled):

```elixir
# config/config.exs
config :super_cache, debug_log: true
```

Runtime:

```elixir
SuperCache.Log.enable(true)
SuperCache.Log.enable(false)
```

### Inspect Internal State

```elixir
# Check partition configuration
SuperCache.Config.get_config(:num_partition)
SuperCache.Config.get_config(:key_pos)
SuperCache.Config.get_config(:partition_pos)

# Check cluster state
SuperCache.Cluster.Manager.live_nodes()
SuperCache.Cluster.Manager.get_replicas(0)

# Check WAL state
SuperCache.Cluster.WAL.stats()

# Check health metrics
SuperCache.Cluster.HealthMonitor.health()
SuperCache.Cluster.HealthMonitor.metrics()

# Check cache statistics
SuperCache.stats()
SuperCache.cluster_stats()
```

### Common Issues

**"tuple size is lower than key_pos"** — Ensure tuples have enough elements for the configured `key_pos`.

**"Partition count mismatch"** — All nodes must have the same `num_partition` value.

**"Replication lag increasing"** — Check network connectivity, verify no GC pauses, use `HealthMonitor.metrics()`.

**"Quorum reads timing out"** — Ensure majority of nodes are reachable, check `:erpc` connectivity.

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

### Adding New Features

1. Add tests first (TDD approach)
2. Implement the feature
3. Update documentation in `README.md` and relevant guides
4. Update `@moduledoc` and `@doc` strings
5. Run full test suite: `mix test`
