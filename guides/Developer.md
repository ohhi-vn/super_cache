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

SuperCache is built on a layered architecture designed for high performance and horizontal scalability. The codebase contains **34 modules** organized into 7 layers:

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

### Complete Module Reference

#### 1. Core API Modules (`lib/api/`)

| Module | Path | Responsibility |
|--------|------|----------------|
| `SuperCache` | `lib/api/super_cache.ex` | Main public API for tuple storage. Handles local/distributed modes transparently. Provides put/get/delete/scan operations with bang and safe variants. |
| `SuperCache.KeyValue` | `lib/api/key_value.ex` | Key-value namespace API with batch operations. Multiple independent namespaces coexist using different `kv_name` values. |
| `SuperCache.Queue` | `lib/api/queue.ex` | Named FIFO queues backed by ETS partitions. Supports add/out/peak/count/get_all operations. |
| `SuperCache.Stack` | `lib/api/stack.ex` | Named LIFO stacks backed by ETS partitions. Supports push/pop/count/get_all operations. |
| `SuperCache.Struct` | `lib/api/struct.ex` | Struct storage with automatic key extraction. Call `init/2` once per struct type before using. |

#### 2. Application & Bootstrap (`lib/`)

| Module | Path | Responsibility |
|--------|------|----------------|
| `SuperCache.Application` | `lib/application.ex` | OTP Application callback. Starts the core supervision tree. Auto-starts cache if `config :super_cache, auto_start: true`. Connects to cluster peers on startup. |
| `SuperCache.Bootstrap` | `lib/bootstrap.ex` | Unified startup/shutdown for both `:local` and `:distributed` modes. Validates options, resolves defaults, starts components in dependency order. |
| `SuperCache.Config` | `lib/app/config.ex` | GenServer-backed configuration store with `:persistent_term` optimization for hot-path keys (`:cluster`, `:key_pos`, `:partition_pos`, `:num_partition`, `:table_type`, `:table_prefix`). |
| `SuperCache.Sup` | `lib/app/sup.ex` | Dynamic supervisor for user-spawned workers and dynamically allocated resources. |
| `SuperCache.Log` | `lib/debug_log.ex` | Zero-cost conditional logging macro. Debug output suppressed by default. Expands to `:ok` when `debug_log: false`. |

#### 3. Buffer System (`lib/buffer/`)

| Module | Path | Responsibility |
|--------|------|----------------|
| `SuperCache.Buffer` | `lib/buffer/buffer.ex` | Manages per-scheduler write buffers for `lazy_put/1`. One buffer process per online scheduler with scheduler affinity via `:erlang.system_info(:scheduler_id)`. |
| `SuperCache.Internal.Queue` | `lib/buffer/queue.ex` | Internal concurrent queue for buffer streams. Supports multiple producers/consumers with graceful shutdown. |
| `SuperCache.Internal.Stream` | `lib/buffer/stream.ex` | Bridges internal queue with caching layer. Creates a `Stream` that pulls items and pushes them into cache. |

#### 4. Partition System (`lib/partition/`)

| Module | Path | Responsibility |
|--------|------|----------------|
| `SuperCache.Partition` | `lib/partition/partition_api.ex` | Handles partition hashing, resolution, and lifecycle. Uses `:erlang.phash2/2` for hashing. Provides `get_partition/1`, `get_partition_order/1`, and partition enumeration. |
| `SuperCache.Partition.Holder` | `lib/partition/partition_holder.ex` | GenServer-backed registry mapping partition indices to ETS table atoms. Uses `:protected` ETS table for lock-free reads. |

#### 5. Storage Layer (`lib/storage/`)

| Module | Path | Responsibility |
|--------|------|----------------|
| `SuperCache.Storage` | `lib/storage/storage_api.ex` | Thin ETS wrapper providing read/write/delete primitives. All tables created with `:write_concurrency` and `:read_concurrency`. Supports `put`, `get`, `get_by_match`, `get_by_match_object`, `scan`, `take`, `delete`, `delete_match`, `update_counter`, `update_element`. |
| `SuperCache.EtsHolder` | `lib/storage/ets_holder.ex` | GenServer that owns the lifecycle of all ETS tables. Tables are automatically deleted on shutdown. |

#### 6. Cluster System (`lib/cluster/`)

| Module | Path | Responsibility |
|--------|------|----------------|
| `SuperCache.Cluster.Bootstrap` | `lib/cluster/cluster_bootstrap.ex` | Distributed mode bootstrap. Handles node connection, config verification across peers, partition map building, and component initialization. |
| `SuperCache.Cluster.Manager` | `lib/cluster/manager.ex` | Cluster membership and partition → primary/replica mapping. Stores partition map in `:persistent_term` for zero-cost reads. Partition assignment: sorted node list rotated by partition index. |
| `SuperCache.Cluster.NodeMonitor` | `lib/cluster/node_monitor.ex` | Monitors declared nodes and notifies Manager when they join/leave. Supports three sources: static `:nodes`, dynamic `:nodes_mfa`, or legacy all-node watching. |
| `SuperCache.Cluster.HealthMonitor` | `lib/cluster/health_monitor.ex` | Continuous health checking via periodic checks of connectivity (RTT), replication lag (probe-based), partition balance, and error rates. Emits `:telemetry` events. |
| `SuperCache.Cluster.Router` | `lib/cluster/router.ex` | Distributed request router. Routes reads/writes to correct nodes. Handles read-your-writes consistency, quorum reads with early termination, and primary routing. |
| `SuperCache.Cluster.Replicator` | `lib/cluster/replicator.ex` | Replication engine with three modes: `:async` (fire-and-forget via Task.Supervisor pool), `:sync` (adaptive quorum), `:strong` (WAL-based). Handles bulk partition transfers. |
| `SuperCache.Cluster.WAL` | `lib/cluster/wal.ex` | Write-Ahead Log for strong consistency. Replaces heavy 3PC with ~200µs latency. Write to local ETS → append to WAL → async replicate → return on majority ack. |
| `SuperCache.Cluster.ThreePhaseCommit` | `lib/cluster/three_phase_commit.ex` | Legacy three-phase commit protocol (PREPARE → PRE_COMMIT → COMMIT). Replaced by WAL but still available for backwards compatibility. |
| `SuperCache.Cluster.TxnRegistry` | `lib/cluster/tnx_registry.ex` | In-memory transaction log for 3PC protocol. Uses `:public` ETS table for lock-free reads. Tracks transaction states: `:prepared` → `:pre_committed` → `:committed`/`:aborted`. |
| `SuperCache.Cluster.Metrics` | `lib/cluster/metrics.ex` | Low-overhead counter and latency sample store. Uses `:public` ETS table with atomic `update_counter/3`. Ring buffer for latency samples (max 256). |
| `SuperCache.Cluster.Stats` | `lib/cluster/stats.ex` | Generates cluster overview, partition maps, 3PC metrics, and API call statistics. Provides pretty-printing for console output. |
| `SuperCache.Cluster.DistributedStore` | `lib/cluster/distributed_store.ex` | Shared routing helpers used by all distributed high-level stores (KeyValue, Queue, Stack, Struct). Provides `route_put`, `route_delete`, `local_get`, `local_match`, etc. |

#### 7. Compatibility Shims (`lib/distributed/`)

All modules in `lib/distributed/` are **deprecated** backwards-compatibility shims that delegate to the unified modules:

| Shim Module | Delegates To |
|-------------|-------------|
| `SuperCache.Distributed` | `SuperCache` |
| `SuperCache.Distributed.KeyValue` | `SuperCache.KeyValue` |
| `SuperCache.Distributed.Queue` | `SuperCache.Queue` |
| `SuperCache.Distributed.Stack` | `SuperCache.Stack` |
| `SuperCache.Distributed.Struct` | `SuperCache.Struct` |

### Performance Optimizations

The codebase includes several performance optimizations:

1. **Compile-time log elimination** — `SuperCache.Log.debug/1` expands to `:ok` when `debug_log: false`
2. **Partition resolution inlining** — `@compile {:inline, get_partition: 1}` eliminates function call overhead
3. **Batch ETS operations** — `:ets.insert/2` with lists instead of per-item calls
4. **Async replication worker pool** — `Task.Supervisor` eliminates per-operation `spawn/1` overhead
5. **Adaptive quorum writes** — Sync mode returns on majority ack, not all replicas
6. **Quorum read early termination** — Stops waiting once majority is reached
7. **WAL-based strong consistency** — Replaces 3PC with fast local write + async replication + majority ack
8. **Persistent-term config** — Hot-path config keys served from `:persistent_term` for O(1) access
9. **Scheduler-affine buffers** — `lazy_put/1` routes to buffer on same scheduler via `:erlang.system_info(:scheduler_id)`
10. **Protected ETS tables** — Partition.Holder uses `:protected` ETS for lock-free reads

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

# Limit failures
mix test --max-failures 3
```

### Cluster Tests

```bash
# Run only cluster tests
mix test test/cluster/

# Run with specific seed for reproducibility
mix test test/cluster/ --seed 12345

# Run via alias (sets up proper VM flags)
mix test.cluster
```

> **Note:** Cluster tests can be flaky due to timing and shared node config. Use `--exclude cluster` for fast local development.

### Test Structure

| Directory | Tests | Description |
|-----------|-------|-------------|
| `test/` | 9 files | Core single-node tests (ETS, KV, Queue, Stack, Struct, Partition, Storage) |
| `test/cluster/` | 9 files | Cluster integration tests (bootstrap, node failure, health monitor, 3PC, RYW) |
| `test/distributed/` | 6 files | Distributed API tests (batch writes, KV, main API, Queue, Stack, Struct) |
| `test/support/` | 1 file | `ClusterCase` - Shared ExUnit case template for cluster tests |

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

Scripts are located in `tools/`:

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
SuperCache.Cluster.HealthMonitor.cluster_health()
SuperCache.Cluster.HealthMonitor.partition_balance()

# Check cache statistics
SuperCache.stats()
SuperCache.cluster_stats()

# Check API metrics
SuperCache.Cluster.Stats.api()
SuperCache.Cluster.Stats.partitions()
```

### Common Issues

**"tuple size is lower than key_pos"** — Ensure tuples have enough elements for the configured `key_pos`.

**"Partition count mismatch"** — All nodes must have the same `num_partition` value.

**"Replication lag increasing"** — Check network connectivity, verify no GC pauses, use `HealthMonitor.cluster_health()`.

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

### Module Design Guidelines

- **Public API functions** — Use `@doc` with examples. Provide both bang (`!`) and safe variants for operations that can fail.
- **erpc entry points** — Use `@doc false` for functions called across nodes via `:erpc`. These must be public but are not part of the user-facing API.
- **Local implementations** — Prefix with `do_local_*` to avoid collision with `@doc false` erpc entry points.
- **Distributed routing** — Use `Cluster.Router` for routing, `Cluster.Replicator` for replication, and `Cluster.DistributedStore` for shared helpers.
- **Configuration** — Store in `Config` GenServer. Use `:persistent_term` for hot-path keys.
- **ETS tables** — Always use `EtsHolder` for lifecycle management. Tables are auto-deleted on shutdown.