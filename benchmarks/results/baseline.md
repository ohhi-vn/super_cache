# Baseline — harden-distributed-reliability

Recorded before the distributed-reliability implementation, on the unmodified library
(apart from prerequisite warning cleanup and the invalid Mix alias fix).

Environment: Elixir 1.20.4, OTP 29, macOS.

## Unit tests

Command:

```sh
mix test
```

Result:

```text
Finished in 48.9 seconds (0.00s async, 48.9s sync)
Result: 825 passed, 141 excluded
```

## Cluster tests

Command:

```sh
mix test.cluster
```

Result:

```text
Finished in 91.4 seconds (0.00s async, 91.4s sync)
Result: 141 passed, 825 excluded
```

The test run passed, but the pre-existing `test.cluster` alias ended with Mix exit 1
because it contained nested `coveralls`/`quality` aliases. That alias definition was
corrected before implementation; the excluded count is the normal unit-test exclusion
of `@tag :cluster`.

## Local ETS benchmark

Command (run twice):

```sh
mix run benchmarks/ets_benchmark.exs
```

Run 1:

```text
match/2: 236µs (236 µs total for 1000 iterations)
select/2: 235µs (235 µs total for 1000 iterations)
Improvement: 0.42% faster with select/2

match_object/2: 212µs (212 µs total for 1000 iterations)
select/2: 216µs (216 µs total for 1000 iterations)
Improvement: -1.89% faster with select/2

tab2list/1: 67µs (67 µs total for 1000 iterations)
select/2 (all): 14µs (14 µs total for 1000 iterations)
Improvement: 79.1% faster with select/2 (all)
```

Run 2:

```text
match/2: 216µs (216 µs total for 1000 iterations)
select/2: 203µs (203 µs total for 1000 iterations)
Improvement: 6.02% faster with select/2

match_object/2: 207µs (207 µs total for 1000 iterations)
select/2: 206µs (206 µs total for 1000 iterations)
Improvement: 0.48% faster with select/2

tab2list/1: 64µs (64 µs total for 1000 iterations)
select/2 (all): 15µs (15 µs total for 1000 iterations)
Improvement: 76.56% faster with select/2 (all)
```

The script reports aggregate totals over 1000 iterations, not percentiles. The
median of the two aggregate runs is used as the comparison baseline because the
script does not expose per-iteration samples. Any future change to the script that
adds percentile output should record both values.

## Distributed benchmark

Baseline is recorded in `benchmarks/results/distributed-baseline.md` after
`benchmarks/distributed_benchmark.exs` is added.
