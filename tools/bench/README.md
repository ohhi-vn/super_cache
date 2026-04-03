# SuperCache Benchmark & Profiling Suite

A comprehensive collection of tools for measuring, profiling, and validating SuperCache performance across single-node and distributed configurations.

## 📑 Table of Contents

- [Quick Start](#quick-start)
- [Tool Overview](#tool-overview)
- [1. Performance Benchmark](#1-performance-benchmark-toolssperformanceexs)
- [2. Profiling Tool](#2-profiling-tool-toolsprofilingexs)
- [3. Benchee Benchmarks](#3-benchee-benchmarks-toolsbenchbenchee_benchmarkexs)
- [4. Distributed Benchmark](#4-distributed-benchmark-toolsbenchdistributed_benchmarkexs)
- [5. Workload Correctness Tests](#5-workload-correctness-tests-toolsbenchworkload_testexs)
- [Environment Variables & CLI Reference](#environment-variables--cli-reference)
- [Tips for Accurate Benchmarking](#tips-for-accurate-benchmarking)
- [Troubleshooting](#troubleshooting)

---

## Quick Start

```bash
# Run the full performance benchmark suite
mix run tools/performance.exs

# Run eprof profiling on write operations
mix run tools/profiling.exs --type eprof --mode write

# Run Benchee benchmarks with custom concurrency
BENCH_CONCURRENCY=100 mix run tools/bench/benchee_benchmark.exs

# Run distributed benchmark across two nodes
BENCH_PEERS="node1@127.0.0.1,node2@127.0.0.1" mix run tools/bench/distributed_benchmark.exs
```

---

## Tool Overview

| Tool | Purpose | Best For |
|------|---------|----------|
| [`performance.exs`](../performance.exs) | CLI-driven throughput & latency benchmarks with memory tracking | Quick performance checks, CI/CD regression testing, CSV export |
| [`profiling.exs`](../profiling.exs) | Deep function-level profiling (`eprof`, `fprof`, `cprof`, memory) | Identifying CPU hotspots, call graph analysis, memory leak detection |
| [`benchee_benchmark.exs`](benchee_benchmark.exs) | Statistical benchmarking with warmup, deviation, and percentiles | Rigorous comparative benchmarking, concurrency scaling analysis |
| [`distributed_benchmark.exs`](distributed_benchmark.exs) | Multi-node throughput & replication latency testing | Cluster sizing, replication mode comparison (`async` vs `sync` vs `strong`) |
| [`workload_test.exs`](workload_test.exs) | Real-world workload simulation with correctness assertions | Validating data integrity under concurrent load |

---

## 1. Performance Benchmark (`tools/performance.exs`)

A fast, CLI-driven benchmark suite that measures throughput, latency percentiles, and memory allocation.

### Usage

```bash
# Run all benchmarks with defaults
mix run tools/performance.exs

# Run specific mode with custom workers
mix run tools/performance.exs --mode write --workers 16

# Export results to CSV
mix run tools/performance.exs --mode all --export-csv results.csv

# Show help
mix run tools/performance.exs --help
```

### CLI Options

| Flag | Alias | Default | Description |
|------|-------|---------|-------------|
| `--mode MODE` | `-m` | `all` | `write`, `read`, `mixed`, `lazy_write`, `kv`, `queue`, `stack`, `memory`, `all` |
| `--workers N` | `-w` | `8` | Number of concurrent worker processes |
| `--records N` | `-n` | `500_000` | Operations per worker |
| `--key-range N` | `-k` | `1_000_000` | Range of random keys for access patterns |
| `--partitions N` | `-p` | `auto` | Number of ETS partitions (defaults to schedulers) |
| `--table-type TYPE` | | `set` | ETS table type: `set`, `bag`, `ordered_set` |
| `--export-csv FILE` | `-o` | `nil` | Export results to CSV |
| `--no-warmup` | | `false` | Disable CPU warmup phase |

### Output Example

```
╔══════════════════════════════════════════════════════════╗
║           SuperCache Performance Benchmark              ║
╚══════════════════════════════════════════════════════════╝

Configuration:
  Mode:       write
  Workers:    8
  Records:    500000 per worker

▶ Running: Write-only...
┌─ Write-only
│  operations=4.0M  avg=12µs  p50=10µs  p95=28µs  p99=45µs
│  throughput=83.3M ops/s  min=4µs  max=120µs
└─────────────────────────────────────────────────────────────
```

---

## 2. Profiling Tool (`tools/profiling.exs`)

Deep-dive profiling using Erlang's built-in tracing and analysis tools.

### Profiler Types

| Type | Description | Overhead | Best For |
|------|-------------|----------|----------|
| `eprof` | Time-based sampling. Shows % of total time per function. | Medium | Finding CPU hotspots |
| `fprof` | Function tracing with call graphs. Shows callers/callees & accumulated time. | High | Understanding call flows & bottlenecks |
| `cprof` | Call count profiling. Counts function invocations. | Low | Detecting excessive calls / N+1 patterns |
| `memory` | Memory allocation tracking. Shows delta per subsystem. | Low | Finding memory leaks & allocation spikes |

### Usage

```bash
# Profile write operations with eprof
mix run tools/profiling.exs --type eprof --mode write

# Profile read operations with fprof and save to file
mix run tools/profiling.exs --type fprof --mode read --output fprof.txt

# Profile memory usage for KV operations
mix run tools/profiling.exs --type memory --mode kv

# Profile with 8 workers
mix run tools/profiling.exs --type cprof --mode mixed --workers 8
```

### CLI Options

| Flag | Alias | Default | Description |
|------|-------|---------|-------------|
| `--type TYPE` | `-t` | `eprof` | `eprof`, `fprof`, `cprof`, `memory` |
| `--mode MODE` | `-m` | `write` | `write`, `read`, `mixed`, `lazy_write`, `kv`, `queue`, `stack` |
| `--workers N` | `-w` | `4` | Concurrent workers |
| `--records N` | `-n` | `100_000` | Operations per worker |
| `--output FILE` | `-o` | `nil` | Save profiler output to file |

---

## 3. Benchee Benchmarks (`tools/bench/benchee_benchmark.exs`)

Statistical benchmarking using the industry-standard [Benchee](https://github.com/bencheeorg/benchee) library. Provides warmup phases, standard deviation, median, and 99th percentile metrics.

> **Note:** This file fixes a critical bug in earlier versions where `Task.async_stream` was evaluated *before* `Benchee.run`, measuring nothing. All work is now properly scoped inside Benchee functions.

### Usage

```bash
# Run all scenarios
mix run tools/bench/benchee_benchmark.exs

# Run only write benchmarks
BENCH_SCENARIO=write mix run tools/bench/benchee_benchmark.exs

# Test concurrency scaling
BENCH_SCENARIO=concurrency mix run tools/bench/benchee_benchmark.exs

# Custom concurrency & record count
BENCH_CONCURRENCY=100 BENCH_RECORDS=100_000 mix run tools/bench/benchee_benchmark.exs
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BENCH_SCENARIO` | `all` | `all`, `write`, `read`, `mixed`, `kv`, `queue`, `stack`, `concurrency` |
| `BENCH_RECORDS` | `50_000` | Operations per benchmark run |
| `BENCH_CONCURRENCY` | `50` | Max concurrent `Task.async_stream` workers |
| `BENCH_WARMUP_S` | `2` | Warmup duration in seconds |
| `BENCH_TIME_S` | `5` | Measurement duration in seconds |

---

## 4. Distributed Benchmark (`tools/bench/distributed_benchmark.exs`)

Measures throughput and latency across multiple Erlang nodes. Tests replication modes, quorum reads, and network forwarding overhead.

### Multi-Node Setup

```bash
# Terminal 1 — Peer A
elixir --sname peer_a --cookie benchcookie -S mix run --no-halt

# Terminal 2 — Peer B
elixir --sname peer_b --cookie benchcookie -S mix run --no-halt

# Terminal 3 — Coordinator (runs benchmark)
BENCH_PEERS="peer_a@127.0.0.1,peer_b@127.0.0.1" \
  elixir --sname coordinator --cookie benchcookie \
  -S mix run tools/bench/distributed_benchmark.exs
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BENCH_PEERS` | `""` | Comma-separated peer node names |
| `BENCH_PARTITIONS` | `8` | ETS partitions per node |
| `BENCH_REP_FACTOR` | `2` | Replication factor (primary + replicas) |
| `BENCH_REP_MODE` | `async` | `async`, `sync`, or `strong` (3PC) |
| `BENCH_DURATION_S` | `5` | Seconds per scenario |
| `BENCH_CONCURRENCY` | `50` | Parallel worker tasks |

---

## 5. Workload Correctness Tests (`tools/bench/workload_test.exs`)

Simulates real-world usage patterns and verifies data integrity under concurrent load. Unlike pure throughput benchmarks, these tests assert correctness.

### Workload Scenarios

| Scenario | What it Tests |
|----------|---------------|
| **Session Cache** | 80% read / 20% write + final state assertion |
| **Event Queue** | Concurrent producers/consumers, zero-loss verification |
| **Leaderboard** | Concurrent struct overwrites + consistency check |
| **Feature Flags** | Read-heavy KV + writer concurrency |
| **Cache Stampede** | 50 readers + 1 writer on the same hot key |
| **Delete Correctness** | Individual delete, `delete_all`, boundary checks |

### Usage

```bash
# Run all workloads locally
mix run tools/bench/workload_test.exs

# Run in distributed mode
WORKLOAD_PEERS="node1@127.0.0.1,node2@127.0.0.1" \
  mix run tools/bench/workload_test.exs
```

---

## Environment Variables & CLI Reference

### Global Benchmark Variables

| Variable | Applies To | Description |
|----------|------------|-------------|
| `ERL_MAX_PORTS` | All | Max open file descriptors (increase for high concurrency) |
| `ERL_FULLSWEEP_AFTER` | All | GC fullsweep interval (tune for memory-heavy tests) |

### Recommended VM Flags for Benchmarking

```bash
# Disable scheduler compaction for stable throughput
elixir +sbt db --erl "+sbwt none +sbwtdcpu none +sbwtdio none" -S mix run tools/performance.exs

# Increase process limit for high-concurrency tests
elixir --erl "+P 1000000" -S mix run tools/bench/benchee_benchmark.exs
```

---

## Tips for Accurate Benchmarking

1. **Warmup Matters**: Always run a warmup phase. Modern CPUs scale frequency dynamically; the first few seconds are often slower.
2. **Close Background Apps**: Browsers, IDEs, and Docker can cause CPU throttling and GC pauses.
3. **Use `+sbt db`**: Disable scheduler balancing during benchmarks to prevent task migration overhead.
4. **Run Multiple Times**: Take the median of 3-5 runs. Outliers are common due to OS scheduling.
5. **Monitor GC**: Use `:erlang.statistics(:garbage_collection)` or the `memory` profiler to ensure GC isn't skewing results.
6. **Distributed Latency**: When testing distributed mode, ensure nodes are on the same physical machine or low-latency network to isolate SuperCache overhead from network RTT.

---

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| `:eprof` shows 0% time | Profiler attached after work completed | Ensure `:eprof.start_profiling()` is called *before* tasks spawn |
| `fprof` hangs | Trace buffer overflow | Reduce `--records` or increase `+tracelog_size` |
| Benchee reports `0.00 ips` | Streams evaluated prematurely | Ensure all work is inside `fn -> ... end` passed to `Benchee.run` |
| Distributed tests fail with `:nodedown` | Cookie mismatch or firewall | Verify `--cookie` matches on all nodes; check `Node.ping/1` |
| Memory profiler shows negative delta | GC ran during measurement | Call `:erlang.garbage_collect()` before and after measurement |

---

## Contributing

When adding new benchmarks:
1. Keep them idempotent (clean up state after running).
2. Add them to `performance.exs` and `benchee_benchmark.exs`.
3. Document CLI flags and expected output in this README.
4. Ensure they pass in both `:local` and `:distributed` modes.