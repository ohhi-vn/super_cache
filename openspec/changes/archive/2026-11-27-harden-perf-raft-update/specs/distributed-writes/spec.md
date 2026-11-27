# Spec Delta

## ADDED Requirements

### Requirement: Raft write backpressure contract

In raft mode, when the engine's bounded pending-command queue is full, writes SHALL be rejected with a documented backpressure error through the same bang/non-bang contract as other write failures, without appending a new log entry.

#### Scenario: Raft pending queue is full

- **WHEN** a raft-mode write arrives while pending uncommitted commands are at the configured maximum
- **THEN** the bang API raises the documented error and the non-bang API returns an error tuple

### Requirement: Durable-write failures propagate in raft mode

A raft-mode write whose log entry cannot be made durable SHALL fail with the documented distributed error rather than reporting success, across every public data-structure API.

#### Scenario: WAL persistence fails

- **WHEN** a raft-mode write cannot persist its log entry durably
- **THEN** the caller receives the documented error and the write is counted as a Raft failure

### Requirement: Single-commit raft batches are preserved under tuning

Enabling throughput tuning such as command batching or replication batching SHALL NOT change the single-commit-per-partition-batch guarantee: a batch of writes for one partition MUST still produce one Raft commit operation from the caller's perspective.

#### Scenario: Tuned batch write to one partition

- **WHEN** a batch of records for one partition commits with batching and pipelining enabled
- **THEN** the caller observes one success or failure result for the partition
