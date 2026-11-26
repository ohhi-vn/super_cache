# Raft vs. other replication modes — distributed benchmark

Measured with `benchmarks/distributed_benchmark.exs`. The script starts an
N-node cluster for each replication mode and reports throughput and per-op
latency for puts, primary reads, and quorum reads.

Environment: Elixir 1.20.5, OTP 29.1.2, macOS (Apple Silicon, 8 schedulers).

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

> **These runs use real multi-voter Raft groups.** Earlier runs silently kept a
> single-voter group because incremental membership joins never caught up; that
> was fixed in `Raft.Group.Reconciler` and `Cluster.Manager` (see
> [Raft membership fix](#raft-membership-fix)). Each Raft run below was verified
> in the log to have started a cluster with 2 or 3 servers as appropriate.

Numbers are single-run measurements; treat small absolute differences as noise.
The 1-node RF=2 figures were recorded before the fix, which does not affect
single-node behaviour.

## `replication_factor: 2` (two-voter Raft group)

### 1 node

| Mode | `put!` | `get! :primary` | `get! :quorum` |
|------|--------|-----------------|----------------|
| async | 0.72µs/op (1,388,889 ops/s) | 0.29µs/op | 7.24µs/op |
| sync | 2.07µs/op (483,559 ops/s) | 0.86µs/op | 13.20µs/op |
| strong | 4.58µs/op (218,150 ops/s) | 1.98µs/op | 15.76µs/op |
| raft | 776.59µs/op (1,288 ops/s) | 2.97µs/op | 6.46µs/op |

### 3 nodes — Raft group formed with 2 servers

| Mode | `put!` | `get! :primary` | `get! :quorum` |
|------|--------|-----------------|----------------|
| async | 23.09µs/op (43,316 ops/s) | 0.76µs/op | 77.92µs/op |
| sync | 105.94µs/op (9,440 ops/s) | 0.45µs/op | 59.75µs/op |
| strong | 106.07µs/op (9,428 ops/s) | 0.46µs/op | 57.13µs/op |
| raft | 1,111.10µs/op (900 ops/s) | 5.63µs/op | 71.99µs/op |

### 5 nodes — Raft group formed with 2 servers

| Mode | `put!` | `get! :primary` | `get! :quorum` |
|------|--------|-----------------|----------------|
| async | 21.17µs/op (47,228 ops/s) | 0.71µs/op | 86.22µs/op |
| sync | 103.17µs/op (9,693 ops/s) | 0.47µs/op | 58.08µs/op |
| strong | 99.42µs/op (10,058 ops/s) | 0.42µs/op | 56.55µs/op |
| raft | 780.18µs/op (1,282 ops/s) | 4.17µs/op | 52.75µs/op |

## `replication_factor: 3` (three-voter Raft group)

### 3 nodes — Raft group formed with 3 servers

| Mode | `put!` | `get! :primary` | `get! :quorum` |
|------|--------|-----------------|----------------|
| async | 16.26µs/op (61,501 ops/s) | 1.08µs/op | 101.48µs/op |
| sync | 101.51µs/op (9,851 ops/s) | 0.47µs/op | 79.38µs/op |
| strong | 122.17µs/op (8,185 ops/s) | 0.42µs/op | 82.07µs/op |
| raft | 6,767.73µs/op (148 ops/s) | 48.49µs/op | 82.46µs/op |

### 4 nodes (N+1 for RF=3) — Raft group formed with 3 servers

| Mode | `put!` | `get! :primary` | `get! :quorum` |
|------|--------|-----------------|----------------|
| async | 28.74µs/op (34,800 ops/s) | 0.64µs/op | 98.47µs/op |
| sync | 116.80µs/op (8,562 ops/s) | 0.34µs/op | 81.20µs/op |
| strong | 118.77µs/op (8,420 ops/s) | 0.37µs/op | 79.28µs/op |
| raft | 6,870.98µs/op (146 ops/s) | 67.19µs/op | 81.46µs/op |

### 5 nodes — Raft group formed with 3 servers

| Mode | `put!` | `get! :primary` | `get! :quorum` |
|------|--------|-----------------|----------------|
| async | 29.24µs/op (34,200 ops/s) | 0.62µs/op | 101.96µs/op |
| sync | 112.41µs/op (8,896 ops/s) | 0.36µs/op | 79.25µs/op |
| strong | 135.02µs/op (7,407 ops/s) | 0.36µs/op | 81.69µs/op |
| raft | 6,886.38µs/op (145 ops/s) | 46.91µs/op | 86.10µs/op |

## Raft slowdown vs. the alternatives

| Cluster | Raft write vs async | vs strong | Raft primary read vs strong | Raft quorum read vs strong |
|---------|--------------------|-----------|----------------------------|----------------------------|
| RF=2, 3 nodes | 48× | 10.5× | 12× | 1.26× |
| RF=2, 5 nodes | 37× | 7.8× | 10× | 0.93× |
| RF=3, 3 nodes | 416× | 55× | 115× | 1.00× |
| RF=3, 4 nodes | 239× | 58× | 182× | 1.03× |
| RF=3, 5 nodes | 236× | 51× | 130× | 1.05× |

## Why reads differ in Raft mode

`SuperCache.Cluster.Router.maybe_raft_barrier/3` runs a `RaftQuery.barrier`
before every local and `:primary` read when `replication_mode: :raft`; the other
modes skip that step. With a real replicated group, the leader is frequently a
*different* node than the benchmark client, so `:primary` reads also pay an
`:erpc` round-trip to the leader. That is why Raft primary reads cost 4–5µs with
two voters and ~47–67µs with three voters, versus ~0.4µs in the other modes.

`:quorum` reads call `local_read` directly on each node and bypass the barrier,
so they cost about the same in every mode; latency is dominated by quorum-task
coordination and the number of replicas consulted (~55–86µs).

## Raft membership fix

Incremental joins never produced a multi-voter group. `raft_ex` rejects
`$ra_promote` while a peer's `match_index` trails the leader's `commit_index`
(`{:error, :not_caught_up}`), and several issues compounded:

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
  and fall back to `form_cluster/3` from the current partition map when a join
  cannot converge.
- `lib/cluster/manager.ex` — removed the duplicate member-preparation path; the
  Reconciler is now the sole owner of `prepare_member/2`.
- `test/raft_adapter_coverage_test.exs` — added tests for promotion retry and
  for promoting an already-present member.

**Caveat:** `form_cluster/3` rebuilds the group from scratch and discards its
Raft log. In these benchmark runs the fallback fired during startup, before any
writes, so the measurements are valid. In a live cluster, triggering it after
writes would lose data; a safer in-place membership repair is still needed
before relying on this fallback in production.

## Observations

- Raft writes are much slower than the in-memory WAL path: ~8–10× slower at two
  voters and ~50–58× slower at three voters. The three-voter runs also pay an
  `:erpc` hop to reach the leader (elected on a peer), which accounts for much
  of the jump from ~0.8–1.1ms to ~6.8ms.
- Raft write cost is essentially flat with cluster size once the voter count is
  fixed (~780–1,111µs at two voters; ~6,768–6,886µs at three).
- Going from `replication_factor` 2 to 3 raises non-Raft writes from ~99–106µs
  to ~102–135µs (one more replica must acknowledge) and quorum reads from
  ~53–78µs to ~79–102µs.
- Primary reads stay on the local fast path in all modes; Raft adds the query
  barrier and, when the client is not the leader, a network round-trip.
- The 1-node `strong` primary-read sample (1.98µs) is an outlier versus
  ~0.3–0.5µs elsewhere.

## Benchmark harness

`benchmarks/distributed_benchmark.exs` accepts `BENCH_RF` to set
`replication_factor` (default 2, matching the previous hard-coded value). No
other benchmark behaviour was changed.
