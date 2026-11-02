# Proposal

## Why

`mix dialyzer` currently exits 2 with pre-existing warnings even though the
test suite, formatter, and Credo pass. The warning volume hides new defects and
blocks the project's `mix quality` alias. The root causes are identifiable and
mechanical, so they should be fixed in a dedicated change rather than suppressed.

## What Changes

- Include the `:logger` application in the Dialyzer PLT so `Logger.__do_log__/4`
  and `Logger.__should_log__/2` resolve.
- Correct the `SuperCache.Storage.MatchSpec.match_spec` type so bodies may hold
  terms, not only tuples, and remove the resulting downstream contract and
  `no_return` warnings.
- Handle the OTP 29 `:erpc.multicall/5` return shape without unreachable pattern
  clauses in `Replicator`.
- Remove unreachable clauses and narrow pattern matches in `Replicator`,
  `Router`, and `DistributedHelpers` that Dialyzer proves dead.
- Guard the optional `:telemetry` call so Dialyzer does not flag a missing
  function.
- Fix the benchmark Mix task's obsolete compiled-match-spec calls and callback
  metadata warning.
- Add a short note to the change documentation recording the clean run.

No runtime behavior, public API, return shapes, or configuration keys change.
This is a pure static-analysis and tooling cleanup, so the change opts out of
spec deltas with `skip_specs: true`.

## Capabilities

### New Capabilities

None. This change only removes static-analysis warnings and does not alter
observable behavior.

### Modified Capabilities

None.

## Impact

- **Tooling**: `mix.exs` Dialyzer configuration.
- **Code**: `lib/storage/match_spec.ex`, `lib/cluster/replicator.ex`,
  `lib/cluster/router.ex`, `lib/cluster/distributed_helpers.ex`,
  `lib/cluster/health_monitor.ex`, and `lib/mix/tasks/benchmark_ets.ex`.
- **Behavior**: none intended. Existing tests must continue to pass unchanged.
- **Dependencies**: no new dependencies; `:logger` is part of Elixir/OTP.
