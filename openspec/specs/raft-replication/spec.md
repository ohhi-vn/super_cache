# raft-replication Specification

## Purpose

Defines the opt-in Raft-consensus replication mode: how writes gain durability through a replicated log, how partition-scoped groups follow cluster membership, and how failures and lifecycle are exposed to callers.

## Requirements

### Requirement: Opt-in raft replication mode

The system SHALL support a `raft` replication mode that is off by default. Selecting it MUST change the write replication mechanism to Raft consensus while leaving `:async`, `:sync`, and `:strong` modes, their defaults, and their behavior unchanged.

#### Scenario: Raft mode is not selected

- **WHEN** a node starts with any existing replication mode and no raft-specific configuration
- **THEN** replication behaves exactly as before and no Raft groups are started

#### Scenario: Raft mode is selected

- **WHEN** a node starts with `replication_mode: :raft`
- **THEN** all replicated writes on that node use the Raft commit path and the node refuses to start if its Raft configuration is invalid

### Requirement: Committed-write durability

In raft mode, a write SHALL NOT be reported as successful before its entry is committed by a Raft majority and applied to the owning partition. If leadership or quorum is lost before commit, the write MUST fail rather than report success.

#### Scenario: Entry reaches quorum

- **WHEN** a write entry is committed by a Raft majority and applied to the partition
- **THEN** the write returns its documented success value

#### Scenario: Quorum is lost before commit

- **WHEN** a write cannot be committed because a majority is unavailable or leadership is lost
- **THEN** the write reports a failure and is not reported as committed

### Requirement: Uniform commit path across data structures

Every public data structure's replicated write SHALL use the same Raft commit mechanism in raft mode. A batch of writes for one partition MUST produce a single Raft commit operation, not one per record.

#### Scenario: Same operation through different APIs

- **WHEN** equivalent writes are performed through the tuple API and a collection API under raft mode
- **THEN** both use the same commit mechanism and failure contract

#### Scenario: Batch write to one partition

- **WHEN** a batch of records all map to one partition under raft mode
- **THEN** the batch produces one Raft commit operation with one result

#### Scenario: Batch write across partitions

- **WHEN** a batch maps to multiple partitions under raft mode
- **THEN** each partition is committed independently and a failed partition does not hide successful ones

### Requirement: Leader routing

A write submitted to a node that is not the partition's Raft leader SHALL be routed to the leader without changing the caller's result shape. If no leader can be reached within the configured timeout, the write MUST fail.

#### Scenario: Write submitted to a follower

- **WHEN** a caller sends a raft-mode write to a node that does not lead the partition's Raft group
- **THEN** the write is forwarded and returns the same success shape as a write sent to the leader

#### Scenario: No reachable leader

- **WHEN** no leader for the partition can be reached within the timeout
- **THEN** the write returns a failure instead of blocking indefinitely

### Requirement: Raft failure normalization

Failures specific to Raft — quorum timeouts, lost leadership, transport errors, and unreachable members — SHALL be normalized to the documented distributed error. Bang APIs MUST raise it and non-bang APIs MUST return `{:error, exception}`; raw exits and transport tuples MUST NOT escape to callers.

#### Scenario: Raft quorum times out

- **WHEN** a raft-mode write times out waiting for a quorum
- **THEN** the bang API raises the documented error and the non-bang API returns an error tuple

#### Scenario: Remote node is unreachable

- **WHEN** a Raft peer is disconnected during a write
- **THEN** the caller receives the documented error rather than a raw exit

### Requirement: Deterministic partition-scoped groups

Each partition SHALL be replicated by one independent Raft group whose members are derived deterministically from the partition's primary and replicas. Nodes sharing the same partition map MUST agree on every group's membership, and membership changes MUST update the groups consistently.

#### Scenario: Nodes agree on group membership

- **WHEN** two nodes have the same partition map
- **THEN** they assign the same members to each partition's Raft group

#### Scenario: Membership changes

- **WHEN** a node joins or leaves the cache cluster
- **THEN** affected Raft groups are updated to reflect the new member set without diverging between nodes

### Requirement: Bootstrap lifecycle and recovery

Raft groups SHALL start only after the cache has bootstrapped and SHALL be released when the cache stops. After a restart, committed log entries MUST be recovered and applied idempotently so replay does not duplicate effects.

#### Scenario: Restart recovers committed entries

- **WHEN** a node restarts under raft mode with entries still present in its Raft log
- **THEN** those entries are recovered and the final partition state matches the committed state

#### Scenario: Cache stops

- **WHEN** the cache is stopped under raft mode
- **THEN** Raft groups are shut down and their resources are released

### Requirement: Read semantics are unchanged

Enabling raft mode SHALL NOT change the semantics or guarantees of the existing read modes. A local read MAY return stale data; primary and quorum reads retain their documented behavior.

#### Scenario: Local read under raft mode

- **WHEN** a caller performs a local read while raft mode is configured
- **THEN** the read uses existing local-read semantics and may return stale data

#### Scenario: Primary read under raft mode

- **WHEN** a caller performs a primary read while raft mode is configured
- **THEN** the read uses existing primary-read semantics

### Requirement: Raft observability

Raft mode SHALL expose its operational state through existing cluster statistics and health reporting. Write failures, leadership changes, and group status MUST be visible to operators.

#### Scenario: Raft stats are available

- **WHEN** an operator queries cluster statistics on a node in raft mode
- **THEN** the report includes Raft group status and write counters

#### Scenario: Raft failure is counted

- **WHEN** a Raft-mode write fails
- **THEN** a Raft failure counter increases and is visible in cluster statistics
