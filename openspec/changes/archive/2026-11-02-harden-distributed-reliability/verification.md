# Verification notes — harden-distributed-reliability

## Tests

Commands:

```sh
mix test
mix test.cluster
```

Results (final run after the hardening changes):

```text
mix test          → 940 passed, 143 excluded, 73.0 s
mix test.cluster  → 143 passed, 940 excluded, 89.8 s
```

No new skips and no failing tests. The excluded sets are the normal
`@tag :cluster` split between the two commands.

### Assertion changes

- `test/coverage_round_four_test.exs` — the forwarded-write test previously
  accepted a raw `ErlangError` or `:erpc` exit. It now asserts the documented
  `SuperCache.Cluster.Error`, because `DistributedHelpers.route_write/4`
  normalizes forwarded failures. No success-shape assertion changed.
- Success return shapes in `test/distributed/*_test.exs` were not edited.

## Formatting, lint, and static analysis

```sh
mix format --check-formatted   → passed (exit 0)
mix credo --strict             → passed, no issues (exit 0)
mix dialyzer                   → Total errors: 0, passed successfully (exit 0)
```

The warning categories originally reported for `Logger`, `MatchSpec`,
`:erpc.multicall/5`, optional telemetry, and the benchmark task were fixed in
the companion change `fix-dialyzer-warnings`; see that change's
`verification.md` for the details. `mix dialyzer` now exits 0 for this change.

## Documentation

```sh
mix docs → exit 0
```

Docs build successfully. ExDoc emits warnings for pre-existing internal
references (`Router.track_write`, `Router.ryw_recent?`,
`Router.resolve_read_mode`, `Config.distributed?`). No broken reference to a
new public module was introduced.

## Public API audit

`git diff` over `lib/api` and `lib/cluster` shows:

- No existing public function signature removed or changed.
- New internal helpers only: `DistributedHelpers.read_or_error/1`,
  `Manager.donor_for/4`, `Replicator.__batch_size__/0` (all `@doc false`).
- `Metrics.increment/3` adds an optional amount argument; `increment/2`
  remains valid through the default.
- No new mandatory configuration keys.

Success return shapes for existing public functions are unchanged.

## Benchmarks

`benchmarks/results/after.md` records the local and distributed benchmark
comparisons. Every measured mode improved; no mode regressed beyond tolerance.
