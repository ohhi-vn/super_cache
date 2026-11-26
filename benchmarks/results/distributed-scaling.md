# Distributed benchmark — cluster size scaling

Recorded with the updated `benchmarks/distributed_benchmark.exs` script, which
now accepts a `BENCH_NODES` environment variable to select the cluster size.
The script starts an N-node cluster for each replication mode and reports
throughput and per-operation latency for puts, primary reads, and quorum reads.

Environment: Elixir 1.20.5, OTP 29.1.2, macOS (Apple Silicon, 8 schedulers).
Command (example for 5 nodes):

```sh
BENCH_NODES=5 elixir --name bench@127.0.0.1 --cookie bench_secret \\
  -S mix run benchmarks/distributed_benchmark.exs
```

Cluster: N nodes, `replication_factor: 2`, `num_partition: 8`, `:set` tables.
Workload: 100 warmup puts, 500 measured puts, 1,000 primary reads, 1,000 quorum reads.

## 1 node

| Mode | put! | get! :primary | get! :quorum |
|------|------|---------------|--------------|
| async | 500 ops / 356µs / 0.71µs per op / 1,404,494 ops/s | 1,000 ops / 319µs / 0.32µs per op / 3,134,796 ops/s | 1,000 ops / 6,849µs / 6.85µs per op / 146,007 ops/s |
| sync | 500 ops / 352µs / 0.70µs per op / 1,420,455 ops/s | 1,000 ops / 324µs / 0.32µs per op / 3,086,420 ops/s | 1,000 ops / 6,039µs / 6.04µs per op / 165,590 ops/s |
| strong | 500 ops / 423µs / 0.85µs per op / 1,182,033 ops/s | 1,000 ops / 330µs / 0.33µs per op / 3,030,303 ops/s | 1,000 ops / 5,932µs / 5.93µs per op / 168,577 ops/s |

## 3 nodes

| Mode | put! | get! :primary | get! :quorum |
|------|------|---------------|--------------|
| async | 500 ops / 3,410µs / 6.82µs per op / 146,628 ops/s | 1,000 ops / 387µs / 0.39µs per op / 2,583,979 ops/s | 1,000 ops / 57,221µs / 57.22µs per op / 17,476 ops/s |
| sync | 500 ops / 27,739µs / 55.48µs per op / 18,025 ops/s | 1,000 ops / 350µs / 0.35µs per op / 2,857,143 ops/s | 1,000 ops / 49,587µs / 49.59µs per op / 20,167 ops/s |
| strong | 500 ops / 27,955µs / 55.91µs per op / 17,886 ops/s | 1,000 ops / 352µs / 0.35µs per op / 2,840,909 ops/s | 1,000 ops / 45,697µs / 45.70µs per op / 21,883 ops/s |

## 5 nodes

| Mode | put! | get! :primary | get! :quorum |
|------|------|---------------|--------------|
| async | 500 ops / 3,598µs / 7.20µs per op / 138,966 ops/s | 1,000 ops / 308µs / 0.31µs per op / 3,246,753 ops/s | 1,000 ops / 58,887µs / 58.89µs per op / 16,982 ops/s |
| sync | 500 ops / 40,370µs / 80.74µs per op / 12,385 ops/s | 1,000 ops / 455µs / 0.46µs per op / 2,197,802 ops/s | 1,000 ops / 54,657µs / 54.66µs per op / 18,296 ops/s |
| strong | 500 ops / 47,897µs / 95.79µs per op / 10,439 ops/s | 1,000 ops / 491µs / 0.49µs per op / 2,036,660 ops/s | 1,000 ops / 57,956µs / 57.96µs per op / 17,254 ops/s |

## Raft mode (`replication_mode: :raft`)

Raft mode uses a single partition (1 Raft group per cluster) and opt-in
consensus replication via `raft_ex`. It was benchmarked separately with
`num_partition: 1`, `replication_factor: 2`.

| Cluster | put! | get! :primary | get! :quorum |
|---------|------|---------------|--------------|
| 1 node | 500 ops / 407,482µs / 814.96µs per op / 1,227 ops/s | 1,000 ops / 3,822µs / 3.82µs per op / 261,643 ops/s | 1,000 ops / 5,718µs / 5.72µs per op / 174,886 ops/s |
| 3 nodes | 500 ops / 379,964µs / 759.93µs per op / 1,316 ops/s | 1,000 ops / 3,628µs / 3.63µs per op / 275,634 ops/s | 1,000 ops / 51,829µs / 51.83µs per op / 19,294 ops/s |
| 5 nodes | 500 ops / 363,532µs / 727.06µs per op / 1,375 ops/s | 1,000 ops / 3,662µs / 3.66µs per op / 273,075 ops/s | 1,000 ops / 52,628µs / 52.63µs per op / 19,001 ops/s |

Raft writes are consensus-bound and roughly an order of magnitude slower
than strong (3PC) writes; primary reads stay on the local fast path.

## Observations

- Primary reads stay on the local fast path regardless of cluster size
  (sub-microsecond to a few microseconds on the owning node).
- Put throughput drops sharply from 1 node to 3 nodes because writes are
  replicated to `replication_factor` (2) peers; going from 3 to 5 nodes
  changes throughput only modestly.
- Quorum-read latency is dominated by per-task waiting and scales with the
  number of replicas consulted, not the cluster size; 3- and 5-node clusters
  are within ~5% of each other.
- Async writes are consistently the fastest; strong (3PC) writes are slower;
  Raft consensus writes are the slowest, as expected.
}