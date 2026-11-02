# Tasks

## 1. Dialyzer configuration

- [x] 1.1 Add `:logger` and `:mix` to `plt_add_apps` in `mix.exs` and verify `mix dialyzer` rebuilds the PLT without `Logger.__do_log__/4`, `Logger.__should_log__/2`, or `Mix.Task` callback warnings.

## 2. Match-spec typing

- [x] 2.1 Widen `SuperCache.Storage.MatchSpec.match_spec` to `[{term(), [term()], [term()]}]` and verify `mix dialyzer` no longer reports invalid-contract or no-return warnings for `MatchSpec`, `Storage`, `Partition`, `Metrics`, or `DistributedStore`.

## 3. Dead clauses and compatibility boundaries

- [x] 3.1 Normalize `:erpc.multicall/5` results through a shared compatibility helper and annotate it for the OTP 29 return shape; verify `mix test test/wal_test.exs test/cluster_local_test.exs` still passes and the replicator pattern warnings are gone.
- [x] 3.2 Remove the unreachable empty-batch `send_batch/4` clause and verify the multi-batch `push_partition/2` tests still pass.
- [x] 3.3 Narrow the accidental `:get` match in the router fan-read path and the `:delete_match` match in `DistributedHelpers`; verify `mix test test/cluster_local_test.exs test/distributed_helpers_test.exs` passes and their pattern warnings are gone.
- [x] 3.4 Annotate the optional `:telemetry.execute/3` boundary and verify `mix dialyzer` no longer reports the unknown telemetry function.

## 4. Benchmark task

- [x] 4.1 Replace compiled match-spec refs with plain specs in `lib/mix/tasks/benchmark_ets.ex` and verify `mix run benchmarks/ets_benchmark.exs` still completes.

## 5. Integration verification

- [x] 5.1 Run `mix format --check-formatted`, `mix credo --strict`, and `mix dialyzer`; verify all three exit 0.
- [x] 5.2 Run `mix test` and `mix test.cluster`; verify the suites pass with the same counts as before the cleanup (940 unit / 143 cluster).
- [x] 5.3 Record the Dialyzer commands and results in `openspec/changes/fix-dialyzer-warnings/verification.md`.

## Workflow follow-up

- Return to `harden-distributed-reliability`, re-run `mix dialyzer`, and mark task 11.4 complete.
- Archive this change after the project's review requirements are satisfied.
