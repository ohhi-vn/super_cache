# Raft vs. other replication modes — distributed benchmark

Measured with `benchmarks/distributed_benchmark.exs`. The script starts an
N-node cluster for each replication mode and reports throughput and per-op
latency for puts, primary reads, and quorum reads.

Environment: Elixir 1.20.5, Erlang/OTP 29 (erts 17.1.1), macOS (Apple Silicon,
8 schedulers), `raft_ex` 0.7.0. Re-run on 2026-11-28 after the
`harden-perf-raft-update` change (raft_ex 0.7 configuration, submit-path
hardening, membership repair, and storage/distributed-path performance work).

```sh
BENCH_NODES=3 elixir --name bench@127.0.0.1 --cookie bench_secret \
  -S mix run benchmarks/distributed_benchmark.exs

# replication_factor is configurable via BENCH_RF (default 2):
BENCH_RF=3 BENCH_NODES=3 elixir --name bench@127.0.0.1 --cookie bench_secret \
  -S mix run benchmarks/distributed_benchmark.exs
```

Workload per mode: 100 warmup puts, 500 measured puts, 1,000 `:primary` reads,
1,000 `:quorum` reads. Raft uses `num_partition: 1` (one Raft group); the other
modes use `num_partition: 8`. Tables are `:set`.

> **These runs use real multi-voter Raft groups.** Each run was checked in the
> log to have reached two voters at `replication_factor: 2` and three voters at
> `replication_factor: 3` before the measured writes. Startup emits transient
> `command NOT appended: not_caught_up` warnings while peers catch up; these
> stop before the benchmark window. The 1-node RF=2 figures are a single-voter
> group and are not directly comparable to the replicated runs.

Numbers are single-run measurements; treat small absolute differences as noise.
Compared with the previous snapshot (recorded before the raft_ex 0.7 bump),
Raft write latency is substantially higher in several configurations. This is
an updated snapshot, not a controlled A/B comparison — leader placement,
catch-up timing, and machine load all move these numbers.

## `replication_factor: 2` (two-voter Raft group)

### 1 node

| Mode | `put!` | `get! :primary` | `get! :quorum` |
|------|--------|-----------------|----------------|
| async | 1.65µs/op (605,327 ops/s) | 0.77µs/op (1,297,017 ops/s) | 19.97µs/op (50,065 ops/s) |
| sync | 11.39µs/op (87,796 ops/s) | 2.84µs/op (352,609 ops/s) | 20.47µs/op (48,847 ops/s) |
| strong | 4.17µs/op (240,038 ops/s) | 1.16µs/op (864,304 ops/s) | 18.50µs/op (54,063 ops/s) |
| raft | 2,800.10µs/op (357 ops/s) | 8.36µs/op (119,589 ops/s) | 13.24µs/op (75,529 ops/s) |

### 3 nodes — Raft group formed with 2 servers

| Mode | `put!` | `get! :primary` | `get! :quorum` |
|------|--------|-----------------|----------------|
| async | 11.00µs/op (90,876 ops/s) | 0.62µs/op (1,605,136 ops/s) | 79.44µs/op (12,588 ops/s) |
| sync | 108.18µs/op (9,244 ops/s) | 0.42µs/op (2,352,941 ops/s) | 70.41µs/op (14,202 ops/s) |
| strong | 92.07µs/op (10,861 ops/s) | 0.49µs/op (2,036,660 ops/s) | 72.98µs/op (13,702 ops/s) |
| raft | 7,251.79µs/op (138 ops/s) | 6.26µs/op (159,668 ops/s) | 84.32µs/op (11,859 ops/s) |

### 5 nodes — Raft group formed with 2 servers

| Mode | `put!` | `get! :primary` | `get! :quorum` |
|------|--------|-----------------|----------------|
| async | 22.99µs/op (43,497 ops/s) | 1.04µs/op (961,538 ops/s) | 88.94µs/op (11,244 ops/s) |
| sync | 56.21µs/op (17,789 ops/s) | 0.33µs/op (3,030,303 ops/s) | 80.76µs/op (12,382 ops/s) |
| strong | 102.41µs/op (9,765 ops/s) | 0.56µs/op (1,779,359 ops/s) | 67.12µs/op (14,899 ops/s) |
| raft | 7,624.89µs/op (131 ops/s) | 4.72µs/op (211,909 ops/s) | 78.00µs/op (12,820 ops/s) |

## `replication_factor: 3` (three-voter Raft group)

### 3 nodes — Raft group formed with 3 servers

| Mode | `put!` | `get! :primary` | `get! :quorum` |
|------|--------|-----------------|----------------|
| async | 17.33µs/op (57,697 ops/s) | 1.15µs/op (868,810 ops/s) | 101.10µs/op (9,891 ops/s) |
| sync | 91.86µs/op (10,886 ops/s) | 0.44µs/op (2,283,105 ops/s) | 90.47µs/op (11,053 ops/s) |
| strong | 120.81µs/op (8,277 ops/s) | 0.36µs/op (2,777,778 ops/s) | 78.66µs/op (12,713 ops/s) |
| raft | 4,402.46µs/op (227 ops/s) | 5.18µs/op (193,087 ops/s) | 82.33µs/op (12,147 ops/s) |

### 4 nodes (N+1 for RF=3) — Raft group formed with 3 servers

| Mode | `put!` | `get! :primary` | `get! :quorum` |
|------|--------|-----------------|----------------|
| async | 15.40µs/op (64,952 ops/s) | 0.73µs/op (1,364,256 ops/s) | 103.92µs/op (9,623 ops/s) |
| sync | 114.26µs/op (8,752 ops/s) | 0.48µs/op (2,087,683 ops/s) | 91.11µs/op (10,976 ops/s) |
| strong | 133.65µs/op (7,482 ops/s) | 0.34µs/op (2,949,853 ops/s) | 84.24µs/op (11,871 ops/s) |
| raft | 4,419.53µs/op (226 ops/s) | 6.16µs/op (162,232 ops/s) | 89.19µs/op (11,212 ops/s) |

### 5 nodes — Raft group formed with 3 servers

| Mode | `put!` | `get! :primary` | `get! :quorum` |
|------|--------|-----------------|----------------|
| async | 27.62µs/op (36,211 ops/s) | 1.02µs/op (978,474 ops/s) | 94.10µs/op (10,627 ops/s) |
| sync | 114.12µs/op (8,763 ops/s) | 0.55µs/op (1,824,818 ops/s) | 88.29µs/op (11,326 ops/s) |
| strong | 121.62µs/op (8,222 ops/s) | 0.36µs/op (2,762,431 ops/s) | 92.99µs/op (10,754 ops/s) |
| raft | 4,677.40µs/op (214 ops/s) | 9.27µs/op (107,933 ops/s) | 96.10µs/op (10,406 ops/s) |

## Raft slowdown vs. the alternatives

| Cluster | Raft write vs async | vs strong | Raft primary read vs strong | Raft quorum read vs strong |
|---------|--------------------|-----------|----------------------------|----------------------------|
| RF=2, 3 nodes | 659× | 78.8× | 13× | 1.16× |
| RF=2, 5 nodes | 332× | 74.5× | 8× | 1.16× |
| RF=3, 3 nodes | 254× | 36.4× | 14× | 1.05× |
| RF=3, 4 nodes | 287× | 33.1× | 18× | 1.06× |
| RF=3, 5 nodes | 169× | 38.5× | 26× | 1.03× |

The "vs async" column is especially sensitive to the async sample and should
be read as an order-of-magnitude indication, not a precise ratio.

## Why reads differ in Raft mode

`SuperCache.Cluster.Router.maybe_raft_barrier/3` runs a `RaftQuery.barrier`
before every local and `:primary` read when `replication_mode: :raft`; the other
modes skip that step. With a real replicated group, the leader is frequently a
*different* node than the benchmark client, so `:primary` reads also pay an
`:erpc` round-trip to the leader. That is why Raft primary reads cost ~5–9µs
here, versus ~0.3–1.2µs in the other modes (the single-node sync sample at
2.84µs is an outlier caused by the one-node startup path).

`:quorum` reads call `local_read` directly on each node and bypass the barrier,
so they cost about the same in every mode; latency is dominated by quorum-task
coordination and the number of replicas consulted (~67–104µs).

## Raft membership fix

Incremental joins originally never produced a multi-voter group. `raft_ex`
rejects `$ra_promote` while a peer's `match_index` trails the leader's
`commit_index` (`{:error, :not_caught_up}`), and several issues compounded:

1. `Reconciler.await_catch_up/3` returned `:ok` as soon as either side omitted
   a metric, so promotion was attempted too early and then abandoned.
2. `Reconciler.reconcile_with_leader/5` used `Enum.each/2` for joins, silently
   discarding join failures.
3. `Cluster.Manager.update_partition_map/2` independently called
   `Group.prepare_member/2`, racing with the Reconciler and force-deleting a
   server the Reconciler had just started.

Changes made:

- `lib/cluster/raft/group/reconciler.ex` — retry promotion against a deadline,
  tolerate `:already_member`/`:already_voter`, propagate catch-up join failures,
  and fall back to `form_cluster/3` when a join cannot converge.
- `lib/cluster/manager.ex` — removed the duplicate member-preparation path; the
  Reconciler is now the sole owner of `prepare_member/2`.
- `test/raft_adapter_coverage_test.exs` — added tests for promotion retry and
  for promoting an already-present member.

**Current behavior:** membership repair distinguishes a departed leader from a
catch-up failure. No-leader paths wait briefly for an election and then reform
via per-member recover-or-start (survivors restart with their logs and terms
intact; joiners start fresh), while a join that cannot catch up refuses
destructive reformation once committed data exists and reports
`{:error, :committed_data_present}`. The transient `not_caught_up` warnings in
these benchmark logs occur before any measured writes, during the startup
join/reconcile window.

## Observations

- Raft writes are much slower than the in-memory WAL path: ~33–79× slower than
  `:strong` with two voters and ~33–39× slower with three voters in this run.
- Raft write cost is flat from 3 to 5 nodes once the voter count is fixed
  (~7,252–7,625µs at two voters; ~4,402–4,677µs at three voters). The 1-node
  single-voter case (2,800µs) is not replicated and is not part of that trend.
- RF=3 writes are generally slower than RF=2; for example, `:strong` writes are
  ~121–134µs at RF=3 versus ~92–102µs at RF=2 for 3–5 nodes.
- Quorum reads stay roughly mode-independent: ~67–89µs at RF=2 and ~78–104µs
  at RF=3.
- Primary reads stay on the local fast path in all modes; Raft adds the query
  barrier and, when the client is not the leader, a network round-trip.

## Benchmark harness

`benchmarks/distributed_benchmark.exs` accepts `BENCH_RF` to set
`replication_factor` (default 2, matching the previous hard-coded value). No
other benchmark behaviour was changed.
