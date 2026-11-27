# Spec Delta

## Purpose

Exposes safe, validated throughput knobs for the Raft engine so operators can tune batching, pipelining, compaction, and snapshot transfer without changing write semantics or risking unbounded memory.

## ADDED Requirements

### Requirement: Validated throughput tuning knobs

The system SHALL accept optional Raft throughput settings for pending-command bound, command batch linger, pipeline depth, replication batch size, snapshot chunk size, segment storage, and compaction thresholds, apply compiled defaults when unset, and reject invalid values at bootstrap before any group starts.

#### Scenario: Valid tuning config starts

- **WHEN** a node starts in raft mode with valid pipeline, batch, snapshot-chunk, and compaction settings
- **THEN** startup succeeds and the effective values are applied to every group the node forms

#### Scenario: Invalid tuning config rejected

- **WHEN** a node starts in raft mode with a non-positive pipeline depth, batch size, pending-command bound, or snapshot chunk size
- **THEN** bootstrap fails with a configuration error and no Raft group is started

### Requirement: Bounded pending-command backpressure

When pending uncommitted Raft commands reach the configured maximum, the system SHALL reject the next write with a documented backpressure error instead of growing memory without bound, and SHALL accept writes again once pending commands drain.

#### Scenario: Pending-command cap reached

- **WHEN** pending uncommitted Raft commands are at the configured maximum
- **THEN** the next raft-mode write is rejected with a backpressure error and no new log entry is appended

#### Scenario: Writes resume after drain

- **WHEN** pending commands drain below the maximum after a backpressure rejection
- **THEN** a subsequent write is accepted and committed normally

### Requirement: Effective tuning is observable

The system SHALL expose the effective Raft tuning values for each node through existing cluster statistics or health reporting so operators can verify what the engine is running with.

#### Scenario: Operator inspects effective tuning

- **WHEN** an operator queries cluster statistics on a raft-mode node
- **THEN** the report includes the effective pipeline, batch, snapshot-chunk, and compaction settings
