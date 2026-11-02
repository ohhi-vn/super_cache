# Distributed baseline — harden-distributed-reliability

Recorded on the unmodified distributed implementation before the reliability refactor.

Command:

```sh
elixir --name bench@127.0.0.1 --cookie bench_secret \
  -S mix run benchmarks/distributed_benchmark.exs
```

Cluster: three nodes, `replication_factor: 2`, `num_partition: 8`, `:set` tables.
Workload: 100 warmup puts, 500 measured puts, 1,000 primary reads, 1,000 quorum reads.

| Mode | put! | get! :primary | get! :quorum |
|------|------|---------------|--------------|
| async | 12,913µs total / 25.83µs per op / 38,721 ops/s | 414µs total / 0.41µs per op / 2,415,459 ops/s | 98,374µs total / 98.37µs per op / 10,165 ops/s |
| sync | 48,759µs total / 97.52µs per op / 10,255 ops/s | 405µs total / 0.41µs per op / 2,469,136 ops/s | 103,586µs total / 103.59µs per op / 9,654 ops/s |
| strong | 147,951µs total / 295.90µs per op / 3,379 ops/s | 411µs total / 0.41µs per op / 2,433,090 ops/s | 95,735µs total / 95.73µs per op / 10,446 ops/s |

The primary-read numbers include partitions owned by the benchmark node, so they
measure the local fast path. Quorum reads wait per task and are the target of the
bounded quorum work. An improvement in quorum-read latency and no regression in
async/sync/strong write throughput are the comparison criteria for task 11.3.
