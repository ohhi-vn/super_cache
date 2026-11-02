# Verification notes — fix-dialyzer-warnings

## Dialyzer

Final command:

```sh
mix dialyzer
```

Result:

```text
Total errors: 0, Skipped: 0, Unnecessary Skips: 0
done (passed successfully)
exit 0
```

Changes that removed the warnings:

- Added `:logger` and `:mix` to the Dialyzer PLT via
  `dialyzer: [plt_add_apps: [:logger, :mix]]` in `mix.exs`.
- Widened `SuperCache.Storage.MatchSpec.match_spec` to
  `[{term(), [term()], [term()]}]`, which cleared the contract, call, and
  `no_return` warnings across Storage, Partition, Metrics, and
  DistributedStore.
- Centralized `:erpc.multicall/5` result normalization and annotated the
  cross-OTP compatibility helpers.
- Removed the genuinely unreachable empty-batch `send_batch/4` clause and the
  impossible `:get` match in the router's local fan-read path.
- Annotated the guarded optional `:telemetry.execute/3` boundary.
- Removed the obsolete compiled-match-spec benchmark path from
  `Mix.Tasks.BenchmarkEts`.

The PLT now lists `:logger` and `:mix`; `mix dialyzer` reuses it on subsequent
runs.

## Formatting and lint

```sh
mix format --check-formatted  → exit 0
mix credo --strict             → no issues, exit 0
```

## Tests

```sh
mix test          → 940 passed, 143 excluded, 71.8 s
mix test.cluster  → 143 passed, 940 excluded, 90.1 s
```

No assertion edits and no behavior change.

## Benchmarks

```sh
mix benchmark_ets              → exit 0
mix run benchmarks/ets_benchmark.exs → exit 0
```

Both the Mix task and the standalone script still run to completion.

## Follow-up

`harden-distributed-reliability` task 11.4 is now unblocked: `mix dialyzer`
passes cleanly.
