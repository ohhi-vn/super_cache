# SuperCache Benchmark & Workload Scripts

## Files

| File | Purpose |
|------|---------|
| `bench/distributed_benchmark.exs` | Throughput + latency benchmark across all ops |
| `bench/workload_test.exs` | Correctness-under-load workload simulation |

---

## Quick start — single node

```bash
# Benchmark
mix run tools/bench/distributed_benchmark.exs

# Workload test
mix run tools/bench/workload_test.exs
```

---

## Multi-node setup

Start two peer nodes in separate terminals, then run the coordinator:

```bash
# Terminal 1 — peer A
elixir --sname peer_a --cookie benchcookie -S mix run --no-halt

# Terminal 2 — peer B
elixir --sname peer_b --cookie benchcookie -S mix run --no-halt

# Terminal 3 — coordinator (runs the benchmark)
BENCH_PEERS="peer_a@127.0.0.1,peer_b@127.0.0.1" \
  elixir --sname coordinator --cookie benchcookie \
  -S mix run tools/bench/distributed_benchmark.exs
```

For the workload test:

```bash
WORKLOAD_PEERS="peer_a@127.0.0.1,peer_b@127.0.0.1" \
  elixir --sname coordinator --cookie benchcookie \
  -S mix run tools/bench/workload_test.exs
```

---

## Benchmark environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BENCH_PEERS` | `""` | Comma-separated peer node names |
| `BENCH_PARTITIONS` | `8` | Number of ETS partitions |
| `BENCH_REP_FACTOR` | `2` | Replication factor |
| `BENCH_REP_MODE` | `async` | `async`, `sync`, or `strong` |
| `BENCH_DURATION_S` | `5` | Seconds per scenario |
| `BENCH_CONCURRENCY` | `50` | Parallel worker tasks |
| `BENCH_PAYLOAD_BYTES` | `64` | Approximate value payload size |

## Workload environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKLOAD_PEERS` | `""` | Comma-separated peer node names |
| `WORKLOAD_DURATION_S` | `10` | Seconds per workload |

---

## Benchmark scenarios

| Scenario | What it measures |
|----------|-----------------|
| Write-only | Raw put throughput |
| Read-local | Local ETS read throughput (no network) |
| Read-primary | Forwarded read throughput |
| Read-quorum | Quorum read throughput (parallel multi-node) |
| Mixed 20% write | Realistic read-heavy workload |
| Mixed 50% write | Balanced read/write |
| Mixed + delete | Write / read / delete interleaved |
| KV write / read | KeyValue abstraction throughput |
| Queue enqueue/dequeue | Queue throughput |
| Stack push/pop | Stack throughput |
| Replication mode comparison | async vs sync vs strong write throughput |
| Concurrency scaling | Throughput at 1, 5, 10, 25, 50, 100, 200 workers |
| Payload size scaling | Throughput at 16 B → 16 KB payloads |

---

## Workload scenarios

| Workload | What it tests |
|----------|---------------|
| Session cache | 80% read / 20% write + correctness assertion |
| Event queue | Concurrent producers and consumers, no-loss check |
| Leaderboard | Concurrent struct overwrites + final-state check |
| Feature flags | Read-heavy KV + writer concurrency |
| Cache stampede | 50 readers + 1 writer on the same hot key |
| Delete correctness | individual delete, delete_all, boundary checks |

---

## Sample output

```
╔══════════════════════════════════════════╗
║    SuperCache Distributed Benchmark      ║
╚══════════════════════════════════════════╝

  running [Write-only]… done
┌─ Write-only
│  ops=312,450  errors=0  duration=5000ms  throughput=62,490.0 ops/s
│  latency (µs):
│    min=12  avg=198  p50=145  p95=620  p99=1240  p99.9=3100  max=8420
└────────────────────────────────────────────────────────────────

  Scenario comparison (throughput ops/s):
  ──────────────────────────────────────────────────────
  Write-only          ██████████████████████████████  62490.0
  Read-local          ██████████████████████████████  198430.0
  Read-primary        ████████████████                51200.0
  ...
```
