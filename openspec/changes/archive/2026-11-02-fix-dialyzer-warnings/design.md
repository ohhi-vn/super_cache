# Design

## Context

See `proposal.md` — Why. The warnings fall into five groups:

1. Every module that logs produces `Logger.__do_log__/4` and
   `Logger.__should_log__/2` unknown-function warnings. The module is present at
   runtime, but `:logger` and `:mix` are not in the Dialyzer PLT.
2. `SuperCache.Storage.MatchSpec.match_spec` declares bodies as `[tuple]`, while
   valid match-spec bodies contain arbitrary terms such as `:"$_"` or `true`.
   This creates contract, call, and `no_return` warnings across Storage,
   Partition, Metrics, and DistributedStore.
3. OTP 29 returns a flat result list from `:erpc.multicall/5`; older OTP
   returns `{replies, bad_nodes}`. The compatibility branch looks unreachable on
   OTP 29.
4. A few clauses are genuinely dead (`send_batch/4` with an empty batch, a
   `:get` branch after a narrower pattern) and some optional calls
   (`:telemetry.execute/3`) cannot be resolved when the optional dependency is
   absent.
5. `Mix.Tasks.BenchmarkEts` passes compiled match-spec refs to `:ets.select/2`
   and lacks Mix.Task callback metadata in the PLT.

## Goals / Non-Goals

**Goals:**

- `mix dialyzer` exits 0 with no warnings.
- Fix root causes instead of ignoring warnings where the correction is local and
  behavior-preserving.
- Keep compatibility with the supported Elixir range (`~> 1.15`), including the
  pre-OTP-29 `:erpc.multicall/5` shape.

**Non-Goals:**

- Changing logging behavior, storage semantics, replication behavior, or public
  APIs.
- Broad Dialyzer flag suppression (`-Wno_...`) or a catch-all ignore file.
- Refactoring modules that only report warnings as a consequence of the
  MatchSpec type.

## Decisions

### 1. Add `:logger` and `:mix` to the PLT

Configure `dialyzer: [plt_add_apps: [:logger, :mix]]` in `mix.exs`. This teaches
Dialyzer about the Elixir Logger and Mix.Task modules without suppressing
unknown-function warnings. The alternative — ignoring `:unknown_function`
globally — would hide genuine missing-function errors.

### 2. Correct the match-spec type

Change `@type match_spec :: [{tuple, [tuple], [tuple]}]` to
`@type match_spec :: [{term(), [term()], [term()]}]`. A match spec is
`{pattern, guards, body}` where each component may contain arbitrary terms. The
narrower type was the cause of the cascading `invalid_contract`, `call`, and
`no_return` warnings. Suppressing contracts was rejected because the declared
type was simply wrong.

### 3. Keep the cross-OTP multicall branch, annotate it

Normalize `:erpc.multicall/5` results through a shared helper that preserves both
the OTP 29 flat-list form and the older `{replies, bad_nodes}` form. Because
Dialyzer narrows the return type on the running OTP, annotate that compatibility
function with `@dialyzer {:nowarn_function, ...}` rather than deleting support for
older runtimes. The alternative — dropping the tuple branch — would reduce
compatibility for a warning-only issue.

### 4. Remove genuinely dead code

- Delete the empty-batch `send_batch/4` clause; `flush_batch/5` already returns
  early for `[]`.
- Narrow the fan-read branch so `:get` is not matched after it has been excluded.
- Narrow `replicate_op/2` matches to the operation shapes `apply_write/3`
  actually accepts.

These are behavior-preserving deletions, not suppressions.

### 5. Guard optional telemetry and benchmark specs

- Keep the `Code.ensure_loaded?/1` guard for `:telemetry` and annotate
  `emit_telemetry/2` so Dialyzer accepts that the optional call is guarded.
- Pass plain match specs to `:ets.select/2` in the benchmark task instead of the
  deprecated compiled-ref path.

## Risks / Trade-offs

- **Adding PLT apps increases PLT build time once.** → Dialyxir rebuilds the
  versioned PLT automatically; subsequent runs reuse it.
- **`nowarn_function` could hide a real future error in the annotated
  functions.** → Limit annotations to the cross-OTP compatibility helper and the
  optional telemetry boundary, and document why each exists.
- **Type correction could reveal downstream type errors.** → The full test
  suite, Credo, and a second Dialyzer run validate the change.
- **No behavior change is intended.** → Existing tests must pass without
  assertion edits; the public API audit remains clean.

## Migration Plan

1. Update the Dialyzer configuration and rebuild the PLT.
2. Fix the MatchSpec type and remove dead clauses.
3. Annotate the cross-OTP and optional-telemetry boundaries.
4. Fix the benchmark task.
5. Re-run `mix format --check-formatted`, `mix credo --strict`,
   `mix dialyzer`, `mix test`, and `mix test.cluster`.
6. Once clean, return to `harden-distributed-reliability` and complete task 11.4.
