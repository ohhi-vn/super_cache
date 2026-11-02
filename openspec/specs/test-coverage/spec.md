# test-coverage Specification

## Purpose

Defines the coverage contract for the library's failure-handling and
cluster-infrastructure modules, so degraded-path behavior is verified by
deterministic tests and every uncovered line is either eliminated or
explicitly justified.

## Requirements

### Requirement: In-scope module coverage thresholds

The default non-distributed test suite SHALL produce the following minimum
per-module line coverage, measured from the `mix test --cover` report:

| Module | Minimum |
| --- | --- |
| `SuperCache.Cluster.WAL.Store` | 100% |
| `SuperCache.Cluster.Error` | 100% |
| `SuperCache.Cluster.Rpc` | 100% |
| `SuperCache.Storage.MatchSpec` | 91% |
| `SuperCache.Internal.Stream` | 88% |
| `SuperCache.Buffer` | 87% |
| `SuperCache.Cluster.Replicator` | 89% |
| `SuperCache.Cluster.HealthMonitor` | 87% |

#### Scenario: Coverage report meets every threshold

- **WHEN** the contributor runs `mix test --cover` and reads the per-module
  percentages from `cover/`
- **THEN** every in-scope module meets or exceeds its minimum
- **AND** no in-scope module's percentage is lower than its pre-change baseline

#### Scenario: A newly added uncovered line is caught

- **WHEN** a change adds executable code to an in-scope module without a test
  that executes it
- **THEN** that module's reported coverage falls below its minimum (or its
  residual allowlist grows), making the regression visible in review

### Requirement: Every uncovered line is justified

Each in-scope module SHALL have no uncovered executable line other than lines
listed in a residual allowlist that states, per line or clause, why the line
cannot be reached from a single non-distributed VM test.

#### Scenario: Residual line is documented

- **WHEN** a line remains uncovered after the new tests
- **THEN** the design's residual table names the line, the behavior guarded,
  and why no single-VM test can trigger it (for example: requires a connected
  peer node, requires an `:erpc` transport fault, or guards a pure scheduling
  race)

#### Scenario: Reachable line is not excused

- **WHEN** a line is reachable through a public function, a GenServer
  callback, injected state, or a documented process fault
- **THEN** it MUST be covered rather than added to the residual allowlist

### Requirement: Fault-path tests are deterministic

Tests for failure and recovery branches SHALL control the trigger rather than
rely on timing races, and SHALL restore global state before finishing.

#### Scenario: Restart-budget test does not race

- **WHEN** a test exhausts the buffer restart budget
- **THEN** it waits for each queue generation to register before killing it
- **AND** it asserts the give-up log path was reached
- **AND** it restarts the buffers so later tests see a live system

#### Scenario: Global state is restored

- **WHEN** a test mutates `:persistent_term` maps, GenServer state, cached
  health rows, or application environment
- **THEN** the original values are restored in `on_exit` or an `after` block
- **AND** the suite remains order-independent under randomized seeds

### Requirement: Coverage tooling covers every in-scope module

The uncovered-line reporting tool SHALL include all eight in-scope modules.

#### Scenario: Report lists misses for each module

- **WHEN** the contributor runs `mix test --cover` followed by the coverage
  reporting script
- **THEN** the report prints uncovered line numbers and source text for
  `SuperCache.Cluster.WAL.Store`, `SuperCache.Cluster.Error`,
  `SuperCache.Cluster.Rpc`, and `SuperCache.Storage.MatchSpec` in addition to
  the four modules already listed

### Requirement: Production behavior is unchanged

This work SHALL NOT change library runtime behavior, public API signatures,
configuration defaults, or dependencies.

#### Scenario: Test-only diff

- **WHEN** the change is reviewed
- **THEN** every modified `lib/` file is limited to test-support tooling under
  `tools/coverage/`, or no `lib/` file is modified at all
- **AND** `mix.exs` dependencies and application configuration are unchanged

#### Scenario: Existing suites still pass

- **WHEN** `mix test` runs after the change
- **THEN** all pre-existing tests pass unchanged
- **AND** `mix test.cluster` remains runnable and unaffected
