# Proposal

## Why

Eight modules sit between 62.5% and 85.7% line coverage in the current
`mix test --cover` report, all below the project's de facto ~95% bar:

| Module | Line coverage |
| --- | --- |
| `SuperCache.Cluster.WAL.Store` | 62.50% |
| `SuperCache.Cluster.HealthMonitor` | 78.90% |
| `SuperCache.Cluster.Error` | 79.31% |
| `SuperCache.Cluster.Replicator` | 80.95% |
| `SuperCache.Storage.MatchSpec` | 83.33% |
| `SuperCache.Internal.Stream` | 84.00% |
| `SuperCache.Buffer` | 84.15% |
| `SuperCache.Cluster.Rpc` | 85.71% |

These modules carry the WAL table lifecycle, cluster health aggregation,
error normalization, retry/quorum replication, buffer self-healing, and the
RPC boundary — exactly the code that runs when a distributed cluster is
degraded. Their least-tested lines are the failure and recovery branches
(`rescue`/`catch`, cached fallbacks, retry budgets, give-up paths), so the
coverage gap is concentrated where a regression would be hardest to notice
until production.

## What Changes

- **Add targeted tests for the uncovered lines in all eight modules**, driven
  through public APIs and documented seams rather than new production code.
- **Cover currently untested public surface**: WAL store table accessors and
  re-initialization; `Cluster.Error` message formatting for non-integer
  partitions, non-atom operations, exception/binary/generic reasons, and the
  generated `exception/1` callback.
- **Cover `Cluster.Rpc`'s default-arity wrappers, exit/throw normalization,
  and non-map/list context fallback**, plus `call!/4`.
- **Exercise `Storage.MatchSpec` list capture extraction and invalid capture
  atoms.**
- **Drive `Internal.Stream` and `Buffer` recovery paths**: individual retry of
  a poisoned batch, non-list stream items, queue-death restart, restart-budget
  exhaustion, and the queue-shutdown timeout branch.
- **Drive `Replicator` bulk paths**: the 500-record batch flush, successful
  `multicall` accounting, rescue when replica data is malformed, and the
  async worker retry branch.
- **Deepen `HealthMonitor` single-node aggregation**: unreachable-node
  fallback (cached and uncached), non-primary partition checks, cycle-deadline
  timeout handling, and timeout classification.
- **Document the residual uncovered lines** that cannot be reached from a
  single, non-distributed VM (real-peer lag measurement, `:erpc` transport
  faults, pure-race branches) with the reason each is excluded, and extend
  `tools/coverage/uncovered.exs` to report the four modules it currently
  omits (`WAL.Store`, `Cluster.Error`, `Storage.MatchSpec`, `Cluster.Rpc`).
- **No production behavior changes and no new dependencies.**

## Capabilities

### New Capabilities

- `test-coverage`: Coverage expectations for the library's failure-handling
  modules — which modules must be fully covered, what counts as an acceptable
  residual, and how coverage is measured and reported.

### Modified Capabilities

<!-- No existing specs in openspec/specs/. -->

## Impact

- **Tests**: new `test/coverage_round_six_test.exs` plus targeted additions to
  `test/wal_test.exs`, `test/cluster_error_test.exs`, `test/cluster_rpc_test.exs`,
  `test/match_spec_test.exs`, `test/buffer_test.exs`,
  `test/health_monitor_deep_test.exs`, and `test/cluster_local_test.exs`.
- **Tooling**: `tools/coverage/uncovered.exs` gains the four missing module
  targets.
- **Production code**: untouched. Public API, configuration, and runtime
  behavior are unchanged.
- **Dependencies**: none added.
- **Runtime**: the suite gains a small number of sequential tests, including
  one restart-budget test that intentionally waits through the buffer
  backoff schedule (~14s).
