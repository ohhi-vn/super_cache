# Spec Delta

## MODIFIED Requirements

### Requirement: Single-donor ownership handoff

When a node gains a partition in a non-Raft replication mode, exactly one deterministic donor SHALL transfer that partition. The system MUST NOT send the same partition from multiple donors. In `raft` mode, a newly added member MUST rebuild the partition from the committed Raft log and MUST NOT receive a direct ETS handoff.

#### Scenario: Node joins and gains ownership

- **WHEN** a joining node becomes a primary or replica for a partition it did not previously hold in a non-Raft mode
- **THEN** one deterministic surviving holder transfers the partition and other holders do not send duplicate copies

#### Scenario: Duplicate handoff is suppressed

- **WHEN** two membership events trigger the same partition transfer
- **THEN** the system coalesces them into one in-flight transfer

#### Scenario: Raft member joins

- **WHEN** a node joins a partition's Raft group in raft mode
- **THEN** no ETS partition handoff is initiated for that partition
- **AND** the member's state is rebuilt from the committed Raft log
