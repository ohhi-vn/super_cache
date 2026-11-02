# After — harden-distributed-reliability

Recorded after the distributed-reliability implementation. The same machine and
scripts as `baseline.md` were used; environment: Elixir 1.20.4, OTP 29, macOS.

## Unit tests

Command:

```sh
mix test
```

Result:

```text
Finished in 73.0 seconds (1.2s async, 71.8s sync)
Result: 940 passed, 143 excluded
```

No new skips. The excluded count is the normal unit-test exclusion of `@tag :cluster`.

## Cluster tests

Command:

```sh
mix test.cluster
```

Result:

```text
Finished in 89.8 seconds (0.8s async, 89.0s sync)
Result: 143 passed, 940 excluded
```

Every cluster test passed, including node failure, handoff, health, and quorum
scenarios.

## Local ETS benchmark

Command (run twice):

```sh
mix run benchmarks/ets_benchmark.exs
```

Run 1:

```text
match/2: 226µs (226 µs total for 1000 iterations)
select/2: 228µs (228 µs total for 1000 iterations)
Improvement: -0.88% faster with select/2

match_object/2: 206µs (206 µs total for 1000 iterations)
select/2: 191µs (191 µs total for 1000 iterations)
Improvement: 7.28% faster with select/2

tab2list/1: 67µs (67 µs total for 1000 iterations)
select/2 (all): 14µs (14 µs total for 1000 iterations)
Improvement: 79.1% faster with select/2 (all)
```

Run 2:

```text
match/2: 217µs (217 µs total for 1000 iterations)
select/2: 203µs (203 µs total for 1000 iterations)
Improvement: 6.45% faster with select/2

match_object/2: 197µs (197 µs total for 1000 iterations)
select/2: 201µs (201 µs total for 1000 iterations)
Improvement: -2.03% faster with select/2

tab2list/1: 64µs (64 µs total for 1000 iterations)
select/2 (all): 13µs (13 µs total for 1000 iterations)
Improvement: 79.69% faster with select/2 (all)
```

Compared with the baseline medians (match 226µs, match_object 206µs,
tab2list 65.5µs), local query performance is unchanged within run-to-run noise.
No mode regressed beyond the agreed tolerance.

## Distributed benchmark

Command:

```sh
elixir --name bench@127.0.0.1 --cookie bench_secret \
  -S mix run benchmarks/distributed_benchmark.exs
```

Cluster: three nodes, `replication_factor: 2`, `num_partition: 8`, `:set` tables.
Workload: 100 warmup puts, 500 measured puts, 1,000 primary reads, 1,000 quorum reads.

| Mode | put! | get! :primary | get! :quorum |
|------|------|---------------|--------------|
| async | 4,205µs total / 8.41µs per op / 118,906 ops/s | 268µs total / 0.27µs per op / 3,731,343 ops/s | 72,346µs total / 72.35µs per op / 13,822 ops/s |
| sync | 37,846µs total / 75.69µs per op / 13,211 ops/s | 355µs total / 0.35µs per op / 2,816,901 ops/s | 67,683µs total / 67.68µs per op / 14,775 ops/s |
| strong | 35,519µs total / 71.04µs per op / 14,077 ops/s | 325µs total / 0.33µs per op / 3,076,923 ops/s | 56,088µs total / 56.09µs per op / 17,829 ops/s |

### Comparison with `distributed-baseline.md`

| Mode | put! before → after | quorum read before → after |
|------|---------------------|----------------------------|
| async | 25.83µs → 8.41µs (3.1× faster) | 98.37µs → 72.35µs (26% faster) |
| sync | 97.52µs → 75.69µs (22% faster) | 103.59µs → 67.68µs (35% faster) |
| strong | 295.90µs → 71.04µs (4.2× faster) | 95.73µs → 56.09µs (41% faster) |

Every measured mode improved; no mode regressed beyond the agreed tolerance.
Primary-read latency remains on the local fast path as expected.
