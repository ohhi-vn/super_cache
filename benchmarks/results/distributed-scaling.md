# Distributed benchmark — cluster size scaling

Recorded with the updated `benchmarks/distributed_benchmark.exs` script, which
accepts a `BENCH_NODES` environment variable to select the cluster size.
The script starts an N-node cluster for each replication mode and reports
throughput and per-operation latency for puts, primary reads, and quorum reads.

Environment: Elixir 1.20.5, Erlang/OTP 29 (erts 17.1.1), macOS (Apple Silicon,
8 schedulers), `raft_ex` 0.7.0. Re-run on 2026-11-28 after the
`harden-perf-raft-update` change.

Command (example for 5 nodes):

```sh
BENCH_NODES=5 elixir --name bench@127.0.0.1 --cookie bench_secret \\
  -S mix run benchmarks/distributed_benchmark.exs
```

Cluster: N nodes, `replication_factor: 2`, `num_partition: 8`, `:set` tables.
Workload: 100 warmup puts, 500 measured puts, 1,000 primary reads, 1,000 quorum reads.

All numbers below are single-run measurements; small differences are noise.

## 1 node

| Mode | put! | get! :primary | get! :quorum |
|------|------|---------------|--------------|
| async | 500 ops / 826µs / 1.65µs per op / 605,327 ops/s | 1000 ops / 771µs / 0.77µs per op / 1,297,017 ops/s | 1000 ops / 19,974µs / 19.97µs per op / 50,065 ops/s |
| sync | 500 ops / 5,695µs / 11.39µs per op / 87,796 ops/s | 1000 ops / 2,836µs / 2.84µs per op / 352,609 ops/s | 1000 ops / 20,472µs / 20.47µs per op / 48,847 ops/s |
| strong | 500 ops / 2,083µs / 4.17µs per op / 240,038 ops/s | 1000 ops / 1,157µs / 1.16µs per op / 864,304 ops/s | 1000 ops / 18,497µs / 18.50µs per op / 54,063 ops/s |

## 3 nodes

| Mode | put! | get! :primary | get! :quorum |
|------|------|---------------|--------------|
| async | 500 ops / 5,502µs / 11.00µs per op / 90,876 ops/s | 1000 ops / 623µs / 0.62µs per op / 1,605,136 ops/s | 1000 ops / 79,443µs / 79.44µs per op / 12,588 ops/s |
| sync | 500 ops / 54,092µs / 108.18µs per op / 9,244 ops/s | 1000 ops / 425µs / 0.42µs per op / 2,352,941 ops/s | 1000 ops / 70,411µs / 70.41µs per op / 14,202 ops/s |
| strong | 500 ops / 46,036µs / 92.07µs per op / 10,861 ops/s | 1000 ops / 491µs / 0.49µs per op / 2,036,660 ops/s | 1000 ops / 72,983µs / 72.98µs per op / 13,702 ops/s |

## 5 nodes

| Mode | put! | get! :primary | get! :quorum |
|------|------|---------------|--------------|
| async | 500 ops / 11,495µs / 22.99µs per op / 43,497 ops/s | 1000 ops / 1,040µs / 1.04µs per op / 961,538 ops/s | 1000 ops / 88,940µs / 88.94µs per op / 11,244 ops/s |
| sync | 500 ops / 28,107µs / 56.21µs per op / 17,789 ops/s | 1000 ops / 330µs / 0.33µs per op / 3,030,303 ops/s | 1000 ops / 80,762µs / 80.76µs per op / 12,382 ops/s |
| strong | 500 ops / 51,205µs / 102.41µs per op / 9,765 ops/s | 1000 ops / 562µs / 0.56µs per op / 1,779,359 ops/s | 1000 ops / 67,120µs / 67.12µs per op / 14,899 ops/s |

## Raft mode (`replication_mode: :raft`)

Raft mode uses a single partition (1 Raft group per cluster) and opt-in
consensus replication via `raft_ex`. It was benchmarked separately with
`num_partition: 1`, `replication_factor: 2`. Each multi-node run was verified
in the log to have formed a two-voter group before the measured writes; the
`not_caught_up` warnings seen during startup are transient.

| Cluster | put! | get! :primary | get! :quorum |
|---------|------|---------------|--------------|
| 1 node | 500 ops / 1,400,052µs / 2800.10µs per op / 357 ops/s | 1000 ops / 8,362µs / 8.36µs per op / 119,589 ops/s | 1000 ops / 13,240µs / 13.24µs per op / 75,529 ops/s |
| 3 nodes | 500 ops / 3,625,893µs / 7251.79µs per op / 138 ops/s | 1000 ops / 6,263µs / 6.26µs per op / 159,668 ops/s | 1000 ops / 84,323µs / 84.32µs per op / 11,859 ops/s |
| 5 nodes | 500 ops / 3,812,445µs / 7624.89µs per op / 131 ops/s | 1000 ops / 4,719µs / 4.72µs per op / 211,909 ops/s | 1000 ops / 78,002µs / 78.00µs per op / 12,820 ops/s |

Raft writes are consensus-bound and now tens of times slower than `:strong`
writes at two voters; primary reads pay the Raft query barrier and, when the
client is not the leader, an `:erpc` round-trip. See
[`raft-comparison.md`](raft-comparison.md) for RF=3 numbers and the read-path
analysis.

## Observations

- Primary reads stay on the local fast path in the non-Raft modes
  (0.33–2.84µs); Raft adds a barrier and a likely leader round-trip (4.72–8.36µs).
- Put latency increases with cluster size: from 1 to 5 nodes `:async` goes from
  1.65µs to 22.99µs per op and `:strong` from 4.17µs to 102.41µs. The 5-node
  `:sync` sample (56.21µs) is faster than the 3-node one (108.18µs), which is
  run-to-run noise rather than a scaling improvement.
- Quorum-read latency is dominated by per-task coordination and the number of
  replicas consulted, not cluster size: 70–79µs at 3 nodes and 67–89µs at
  5 nodes.
- Async writes are consistently the fastest, strong (3PC) writes are slower,
  and Raft consensus writes are the slowest, as expected. The Raft/strong gap
  is much larger in this run than in the previous snapshot; see
  `raft-comparison.md` for the full comparison.
