# distributed-writes Specification

## Purpose

Defines the consistency, failure-reporting, and recovery guarantees for writes performed while SuperCache is running in distributed mode.

## Requirements

### Requirement: Single strong-write protocol

The system SHALL apply every `:strong` write through one replication protocol for all public data structures. It MUST NOT select different strong-consistency implementations based on the calling API or operation shape.

#### Scenario: Strong write through different APIs

- **WHEN** a caller performs a `:strong` write through the tuple API and through a key-value, queue, stack, or struct API
- **THEN** both writes use the same replication protocol and expose the same failure contract

#### Scenario: Strong write is not silently downgraded

- **WHEN** `:strong` replication is configured
- **THEN** the write is not completed through an unrelated commit mechanism that the configuration does not describe

### Requirement: Primary-inclusive quorum

The quorum for a strong or sync write SHALL include the primary's local apply and SHALL count each replica at most once. Duplicate acknowledgements from the same replica MUST NOT advance the quorum.

#### Scenario: Duplicate acknowledgement does not satisfy quorum

- **WHEN** a partition has one primary and two replicas and one replica sends the same acknowledgement twice while the other replica has not acknowledged
- **THEN** the write does not report success until a majority of all copies has acknowledged

#### Scenario: Majority succeeds with one unavailable replica

- **WHEN** a partition has one primary and two replicas and exactly one replica is unavailable
- **THEN** the write reports success after the primary and the remaining replica have applied it

### Requirement: Write failure propagation

Strong and sync writes SHALL report failure when the required quorum is not reached. Bang APIs SHALL raise a documented exception, and non-bang APIs SHALL return `{:error, exception}`. A write MUST NOT return `true` or `:ok` after a quorum failure.

#### Scenario: All replicas unavailable

- **WHEN** a sync or strong write cannot reach enough replicas to form a quorum
- **THEN** the bang API raises the documented distributed error and the non-bang API returns an error tuple

#### Scenario: Forwarded write failure

- **WHEN** a write is forwarded to a primary that cannot complete replication
- **THEN** the forwarding node surfaces the failure to its caller instead of reporting success

### Requirement: Async replication observability

Async writes SHALL return after local apply and supervised handoff. Replica delivery failures SHALL be logged and counted, and the caller's mailbox MUST NOT receive unconsumed task-result messages.

#### Scenario: Async write to an unavailable replica

- **WHEN** an async write is applied locally and a replica is unavailable
- **THEN** the write returns success, a replication-error counter increases, and the caller's message queue does not grow by one task-result message per write

#### Scenario: Async worker crash

- **WHEN** an async replication worker exits abnormally
- **THEN** the failure is recorded and does not crash the caller or the replication supervisor

### Requirement: Strong-write recovery and idempotency

The system SHALL retain each strong write's log entry until its quorum outcome is known, remove committed entries, and replay uncommitted entries when the replication component restarts. Replay MUST be safe to apply more than once for put and delete operations.

#### Scenario: Committed entry is not replayed

- **WHEN** a strong write reaches quorum and the replication component restarts
- **THEN** the committed entry is absent from recovery and is not re-applied

#### Scenario: Uncommitted entry is replayed

- **WHEN** a strong write is logged but does not reach quorum before the replication component restarts
- **THEN** recovery re-applies the entry and the final cached state reflects it

### Requirement: Strong-write backpressure

The system SHALL reject new strong writes with `{:error, :wal_backpressure}` when pending uncommitted entries exceed the configured maximum, instead of growing memory without bound.

#### Scenario: Pending-entry cap reached

- **WHEN** the number of pending uncommitted entries is at the configured maximum
- **THEN** the next strong write is rejected with `:wal_backpressure` and no new entry is appended

### Requirement: Batched strong writes

A batch of strong writes destined for one partition SHALL be committed in a single quorum operation. The system MUST NOT perform one full quorum operation per record.

#### Scenario: Batch write to one partition

- **WHEN** a caller writes a batch of records that all map to one partition under `:strong` replication
- **THEN** the batch produces one quorum operation and one success or failure result

#### Scenario: Batch write across partitions

- **WHEN** a caller writes a batch that maps to multiple partitions
- **THEN** each partition receives one quorum operation and a failed partition is reported without hiding successful partitions

### Requirement: Distributed write error context

Distributed write failures SHALL be reported as a documented exception containing at least the operation, partition index, and failing node or reason. Remote exits and timeouts MUST be normalized and MUST NOT leak as raw exit signals.

#### Scenario: Primary is unreachable

- **WHEN** a write must be forwarded to a primary that is not reachable
- **THEN** the caller receives the documented exception with operation and node context rather than an unhandled exit

#### Scenario: Replica times out

- **WHEN** a replica does not acknowledge within the configured timeout
- **THEN** the failure is recorded with the partition and replica identity

### Requirement: Public write compatibility

Existing public distributed write functions SHALL preserve their success return shapes and documented options. The change MUST NOT require new mandatory configuration keys.

#### Scenario: Existing call succeeds unchanged

- **WHEN** an application calls an existing public write function with its documented arguments
- **THEN** a successful call returns the same value and shape as before the change

#### Scenario: Existing configuration remains valid

- **WHEN** an application uses the documented replication options without adding new keys
- **THEN** the cache still starts and operates in distributed mode

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
