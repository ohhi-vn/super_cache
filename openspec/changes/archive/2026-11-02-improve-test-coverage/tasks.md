# Tasks

## 1. Baseline and reporting

- [x] 1.1 Extend the `targets` map in `tools/coverage/uncovered.exs` with
      `SuperCache.Cluster.WAL.Store`, `SuperCache.Cluster.Error`,
      `SuperCache.Storage.MatchSpec`, and `SuperCache.Cluster.Rpc`; verify the
      script prints a section for each after `mix test --cover`.
- [x] 1.2 Record the current per-module percentages and miss-line sets for the
      eight modules from a clean `mix test --cover` run (save the numbers in the
      change notes or a scratch file); verify they match the design's baseline
      table (595 executable lines, 479 covered).

## 2. High-value quick wins

- [x] 2.1 In `test/wal_test.exs`, add a describe block that calls
      `Store.wal_table/0`, `ack_table/0`, `watermark_table/0`, and `ryw_table/0`,
      asserts `Store.start_link()` returns `{:error, {:already_started, _}}`
      while the store runs, and calls `Store.init([])` directly to cover the
      existing-table branch; verify `mix test test/wal_test.exs` passes and the
      module reports 100%.
- [x] 2.2 In `test/cluster_error_test.exs`, add cases for
      `Error.exception(reason: :timeout)`, a non-integer partition (string),
      a non-atom operation term, a `%RuntimeError{}` reason, a binary reason,
      and a generic term reason; verify `mix test test/cluster_error_test.exs`
      passes and the module reports 100%.
- [x] 2.3 In `test/cluster_rpc_test.exs`, add a module with functions that
      `exit/1` and `throw/1`, assert `Rpc.call/4` and `Rpc.call!/4` (four-arity
      defaults) return/raise correctly, assert the exit and throw calls
      normalize to `%Error{}` with `{:exit, _}` / `{:throw, _}` reasons, and
      assert `Rpc.normalize(:timeout, nil)` uses the empty-context fallback;
      verify `mix test test/cluster_rpc_test.exs` passes and the module reports
      100%.
- [x] 2.4 In `test/match_spec_test.exs`, add `extract_bindings/1` cases for a
      list element (`[{:"$2", :"$1"}, :"$3"]` ordering) and an invalid capture
      atom (`:"$x"`); verify `mix test test/match_spec_test.exs` passes and the
      module reports at least 91% with only lines 139 and 142 uncovered.

## 3. Buffer and stream recovery

- [x] 3.1 Create `test/coverage_round_six_test.exs` (`async: false`, module
      timeout 120 s) with a helper that reads buffer names from
      `:persistent_term`, waits for a named queue PID with a backoff-aware
      timeout, and restarts buffers in `on_exit`; verify the empty module
      compiles with `mix test test/coverage_round_six_test.exs`.
- [x] 3.2 Add a stream test: feed `Internal.Stream.make_stream_pipe/1` a stream
      of a bare tuple (non-list) and assert the tuple lands in the cache and
      the module reports 88% with only lines 107, 108, and 139 uncovered;
      verify with `mix test test/coverage_round_six_test.exs` and the reporting
      script.
- [x] 3.3 Add a buffer shutdown-timeout test: stop buffers, register a dummy
      process under the first buffer name that ignores `:stop`, start buffers
      (runners attach to the dummy), call `Buffer.stop/0`, assert it returns
      `:ok` after the 1 s timeout; before finishing, kill the dummy and restart
      buffers; verify the test passes and `Buffer` line 326 is covered.
- [x] 3.4 Replace the ineffective restart-budget loop in
      `test/coverage_round_five_test.exs` with a deterministic version (in the
      new file or in place) that captures logs, kills the first buffer queue 11
      times while waiting `min(250 * restarts, 5000) + 1000` ms for each
      generation, asserts the "exceeded 10 consecutive restarts" log appears,
      then restarts buffers and asserts a live queue PID; verify `Buffer` lines
      249–250 are covered.
- [x] 3.5 Confirm the residual list for this group (Buffer 97–98, 136–137,
      218, 220, 230, 278, 288–289; Stream 107–108, 139) matches the report and
      update `design.md` if any listed line is actually covered; verify no
      unexplained Buffer/Stream miss remains.

## 4. Replicator bulk and failure paths

- [x] 4.1 In `test/coverage_round_six_test.exs`, add a batch-flush test that
      inserts 501 records into a partition and calls
      `Replicator.push_partition/2` with `node()` as the target, asserting
      `:ok` and that at least the 500-record batch path ran; verify Replicator
      lines 148–149 are covered.
- [x] 4.2 Add a `replicate_batch/3` success test with a planted map
      `%{idx => {node(), [node()]}}`, asserting `:ok` so successful reply
      accounting runs; OTP 29 returns a flat `multicall` list, so the classic
      `{replies, bad_nodes}` clause (line 226) is a documented residual rather
      than covered.
- [x] 4.3 Add a `replicate_batch/3` malformed-replica test that plants a
      non-list replica value, asserts `{:error, _}`, and covers the rescue at
      line 245.
- [x] 4.4 Add an async replication success test with `node()` as the replica,
      polling storage until the replicated record lands, so the flat multicall
      reply path runs; the classic tuple branch (line 337) is dead on OTP 29
      and stays on the residual list.
- [x] 4.5 Add an async replication retry test with a non-list replica value,
      polling `Cluster.Metrics` until `async_failures` increases, covering the
      rescue and retry block (lines 365–376).
- [x] 4.6 Add a pool-unavailable test: terminate the
      `SuperCache.Cluster.Replicator.Pool` child of `SuperCache.Supervisor`,
      start a temporary `Task.Supervisor` with the same name and
      `max_children: 0`, call `Replicator.replicate/3` in `:async` mode, assert
      `:ok` plus an `async_drops` metric increment, then stop the temporary pool
      and restart the supervised child; verify lines 436–441 are covered and no
      later test fails because the pool is missing.
- [x] 4.7 Add an async replication drop test with an unreachable replica
      (fake node name), polling `Cluster.Metrics` until `async_drops` increases;
      this covers the exhausted-retry and drop-count lines 348, 358–359 that the
      clean baseline leaves uncovered.
- [x] 4.8 Confirm the Replicator residual list (170, 188, 190, 193–194,
      226, 337, 390–391, 395, 397–398, 400, 415–416, 469) matches the report;
      verify the module reports at least 89%.

## 5. HealthMonitor single-node depth

- [x] 5.1 In `test/health_monitor_deep_test.exs`, add a test that injects a
      fake member into `Cluster.Manager` state with `:sys.replace_state/2`,
      calls `cluster_health/0` and `node_health/1` for that node, asserts the
      result is `:unknown` with the documented fallback check map, and restores
      Manager state in `on_exit`; verify HealthMonitor line 131, 189, and the
      remote fallback lines 194–206, 211, 216 are covered.
- [x] 5.2 Add a cached-aggregate test: insert
      `{{fake_node, :aggregate}, health}` into `HealthMonitor.HealthData`, call
      `cluster_health/0`, assert the returned node is `:degraded`; verify lines
      211–213 are covered.
- [x] 5.3 Add a non-primary test: plant a partition map whose primaries are a
      fake node for every partition, run `force_check/0`, and assert the local
      `:replication` and `:partitions` checks are `:unknown` with
      `reason: :not_primary`; verify lines 574 and 633 are covered.
- [x] 5.4 Add a cycle-deadline test: set `cycle_deadline_ms: 0` in the monitor's
      state with `:sys.replace_state/2`, call `force_check/0`, assert
      connectivity is recorded `:fail` (and replication/partitions/error_rate
      `:unknown`), then restore the original opts in `after`; verify lines
      494–496 and 515–519 are covered and the next scheduled cycle still runs.
- [x] 5.5 Confirm the HealthMonitor residual list (198–199, 203, 235, 279–280,
      295–296, 305–306, 373, 380–381, 391, 400, 542–543, 546, 550, 561,
      614–615, 620, 693–694, 705, 787, 802–804) matches the report; verify the
      module reports at least 87%.

## 6. Integration verification

- [x] 6.1 Run `mix test` with three different seeds (for example
      `--seed 0`, `--seed 12345`, `--seed 99999`) and verify the whole suite
      passes with no leaked buffer, Manager state, or pool failures.
- [x] 6.2 Run `mix test --cover` and the extended
      `tools/coverage/uncovered.exs`; verify every module meets its spec
      threshold (WAL.Store 100, Error 100, Rpc 100, MatchSpec ≥91,
      Stream ≥88, Buffer ≥87, Replicator ≥89, HealthMonitor ≥87) and that the
      only uncovered lines are the design's residual allowlist.
- [x] 6.3 Run `mix test.cluster` (or confirm the cluster tag remains untouched
      if the environment cannot run peers) and verify no cluster test depends on
      state changed by the new tests.
- [x] 6.4 Run `mix format --check-formatted` on the added/modified test files
      and verify it passes.
- [x] 6.5 Update `design.md`'s residual table and baseline/expected numbers if
      the measured result differs, and verify the change contains no `lib/`
      modifications outside `tools/coverage/`.
