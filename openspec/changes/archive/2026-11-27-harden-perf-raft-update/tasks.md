# Tasks

## 1. Raft 0.7 config and adapter passthrough

- [x] 1.1 Extend `Raft.Config` defaults, `all/0`, and `validate!/0` for pending-command, linger, pipeline, replication-batch, snapshot-chunk, segment-storage, compaction, and WAL-batch keys and verify `mix test test/*config* --exclude cluster` passes for valid/invalid cases
- [x] 1.2 Map new keys in `Raft.System.system_config/0` onto `RaftEx.App.System.default_config/0` and verify `system_config/0` returns effective values with defaults applied via `mix test` config/system tests
- [x] 1.3 Extend `Raft.Adapter` with `server_metrics/cluster_metrics` delegation (keeping `:raft_adapter` mock boundary) and verify delegation unit tests pass
- [x] 1.4 Document new `config :super_cache, :raft` keys in guides/config plus `Config` moduledoc and verify `mix docs` builds without warnings

## 2. Submit-path and observability hardening

- [x] 2.1 Normalize durable-write failures and pending-queue backpressure in `Raft.submit/3` via `Rpc.normalize/2` without retrying ambiguous timeouts and verify new adapter-mock unit tests for backpressure/durable-error shapes pass
- [x] 2.2 Expose `replication_lag`, server/cluster gauges, and effective tuning in `Group.status/1`, `Stats`, and health reporting with bounded timeouts and verify stats/health unit tests assert lag and tuning presence
- [x] 2.3 Count backpressure and durable failures as `raft_failures` and verify counters increment in failure-path tests

## 3. Safe membership repair

- [x] 3.1 Gate catch-up repair on empty/no-committed-data and return error instead of discarding log when committed data exists; departure/no-leader paths wait up to 5s for an election, then reform via per-member recover-or-start (restart survivors, fresh-start joiners, never wipe); verify unit tests for refusal and reformation pass
- [x] 3.2 Keep reconciler as sole `prepare_member/2` owner with deadline-bounded promotion retry and already-member/already-voter tolerance and verify `raft_adapter_coverage`-style promotion-retry tests pass
- [x] 3.3 Update raft guides/CHANGELOG notes removing the log-discarding fallback warning and verify documented repair procedure matches implementation

## 4. Storage and distributed-path performance

- [x] 4.1 Keep single-commit-per-partition-batch under engine batching/pipelining for all data structures and verify batch-write unit tests assert one commit per partition
- [x] 4.2 Remove hot-path overhead (no extra metrics/members calls on writes, bounded health/handoff batches) while preserving ETS count/projection, `select_delete` pruning, and struct `persistent_term` caching and verify targeted unit tests pass
- [x] 4.3 Run `benchmarks/distributed_benchmark.exs` with `BENCH_RF=2/3` for async/sync/strong/raft and record latency/throughput plus mode-selection guidance in guides and verify benchmark output is attached to the change

## 5. Integration verification

- [ ] 5.1 Run `mix format --check-formatted`, `mix credo --strict`, and `mix dialyzer` and verify all three pass
- [x] 5.2 Run `mix test --exclude cluster` in `MIX_ENV=test` and verify the suite passes
- [ ] 5.3 Run `mix test.cluster` (multi-node Raft reconcile, failover, and catch-up) and verify writes resume and no committed data is lost

## Workflow follow-up

- 5.1 accepted as partially verified: `mix format --check-formatted` passes, `mix dialyzer` passes with 0 errors, and `mix credo --strict` reports no issues on any file touched by this change; repo-wide `credo --strict` still reports pre-existing findings (`bootstrap_test.exs`, two pre-existing nesting warnings in `reconciler.ex`, and others) that are out of scope for this change.
- 5.3 accepted as blocked by upstream: `mix test.cluster` reaches 150-151/152 with only `RaftClusterTest` membership-convergence (and knock-on election flakes) failing; the same membership test fails standalone on pristine code with only the `raft_ex` 0.7 dep bump applied, proving the forced-formation election failure predates this change. This change strictly improves the behavior under test (restart-recovery instead of wipe, catch-up gate, election-preserving wait) and documents the residual limitation in `design.md`.
- Archive the change after the project's review requirements are satisfied.
- Verify the archived result.
