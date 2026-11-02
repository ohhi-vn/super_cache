# Spec Delta

## Purpose

Defines the consistency, failover, and failure-reporting guarantees for reads performed while SuperCache is running in distributed mode.

## ADDED Requirements

### Requirement: Primary-read failover

When a primary read cannot reach the primary, the system SHALL fall back to the local replica only when the local node holds a replica for that partition. Otherwise it SHALL report a read failure.

#### Scenario: Local replica exists

- **WHEN** a primary read is requested, the primary is unreachable, and the local node holds a replica for the partition
- **THEN** the read returns the local replica's value and records a degraded-read event

#### Scenario: Local replica does not exist

- **WHEN** a primary read is requested, the primary is unreachable, and the local node does not hold a replica for the partition
- **THEN** the read reports the documented failure instead of returning an empty result

### Requirement: Quorum includes every copy and accepts nil

Quorum reads SHALL count the primary and every replica once and SHALL treat `nil` as a valid result. A strict majority of all copies is required.

#### Scenario: Nil is the majority result

- **WHEN** a quorum read receives `nil` from two of three copies and a non-nil value from the third
- **THEN** the read returns `nil` as the quorum result

#### Scenario: Non-nil value is the majority result

- **WHEN** a quorum read receives the same non-nil value from a majority of copies
- **THEN** the read returns that value without waiting for the remaining copies

### Requirement: Bounded quorum reads

Quorum reads SHALL complete within a bounded deadline regardless of the number of replicas, SHALL terminate remaining work on early success, and SHALL kill remaining tasks on timeout.

#### Scenario: One replica hangs

- **WHEN** one replica never responds and the remaining copies can form a quorum
- **THEN** the read returns within the deadline using the responding copies and does not leave the hanging task running

#### Scenario: No quorum before the deadline

- **WHEN** no strict majority forms before the deadline
- **THEN** the read stops waiting and follows the documented fallback or failure behavior

### Requirement: No silent empty read results

Transport failures, timeouts, and missing partitions MUST NOT be converted into an empty result. The system SHALL return the documented error or fall back to an available replica and mark the read degraded.

#### Scenario: Primary transport failure

- **WHEN** a primary read fails because the remote call exits or times out and no local replica is available
- **THEN** the caller receives the documented failure rather than `[]`

#### Scenario: Quorum transport failures

- **WHEN** every quorum-read attempt fails at the transport layer
- **THEN** the caller receives the documented failure and an error counter is incremented

### Requirement: Quorum fallback

If no quorum is reached, the system SHALL fall back to the primary's result when available and record a degraded metric. If the primary is unavailable, the system SHALL report a failure.

#### Scenario: Primary result is available

- **WHEN** quorum reads cannot form a majority but the primary returns a value
- **THEN** the read returns the primary's value and records that the quorum was not reached

#### Scenario: Primary result is unavailable

- **WHEN** quorum reads cannot form a majority and the primary is unreachable
- **THEN** the read reports the documented failure

### Requirement: Read-your-writes

After a successful write, subsequent local reads from the same process for the affected partition SHALL be routed to the primary for the configured read-your-writes TTL.

#### Scenario: Read after write with async replication

- **WHEN** a process writes a value under async replication and immediately reads it with the default read mode
- **THEN** the read is routed to the primary and observes the written value

#### Scenario: TTL expires

- **WHEN** the read-your-writes TTL has elapsed and no newer write has occurred
- **THEN** the read may return to local mode and serve the local replica

### Requirement: Read error contract

Bang read APIs SHALL raise the documented distributed exception, and non-bang read APIs SHALL return `{:error, exception}`. A read MUST NOT leak a raw exit signal to the caller.

#### Scenario: Bang read fails

- **WHEN** a bang read cannot satisfy its read mode
- **THEN** it raises the documented exception with operation and partition context

#### Scenario: Non-bang read fails

- **WHEN** a non-bang read cannot satisfy its read mode
- **THEN** it returns `{:error, exception}` and logs the failure

### Requirement: Read mode compatibility

Existing public read functions and `:read_mode` options SHALL keep their documented success return shapes. A successful `:local`, `:primary`, or `:quorum` read SHALL return the same result shape as before the change.

#### Scenario: Existing read succeeds unchanged

- **WHEN** an application calls an existing public read function with a documented read mode
- **THEN** a successful call returns the same value and shape as before the change
