# Spec Delta

## ADDED Requirements

### Requirement: Deadline-bounded safe raft reconciliation

Raft membership reconciliation SHALL be the sole owner of member preparation, SHALL retry promotion against a deadline while tolerating already-member and already-voter outcomes, and SHALL propagate join failures instead of silently discarding them.

#### Scenario: Promotion retries to deadline

- **WHEN** a joining member is not yet caught up during reconciliation
- **THEN** promotion is retried until the deadline instead of failing on the first attempt

#### Scenario: Idempotent membership outcomes

- **WHEN** reconciliation observes an already-member or already-voter result
- **THEN** it treats the outcome as success and continues toward a multi-voter quorum

### Requirement: No destructive catch-up fallback after committed writes

When a joining member fails to catch up while committed writes exist for a
partition, the system SHALL NOT rebuild its Raft group from scratch with an
empty log as a repair fallback. Departure-driven reformation (no leader
after a member leaves, or a gone leader process) is still permitted so the
group converges from the current partition map.

#### Scenario: Committed data blocks destructive catch-up rebuild

- **WHEN** a joining member cannot converge while committed entries exist
- **THEN** the system reports a failure instead of discarding the log

#### Scenario: Departure still converges

- **WHEN** a member departs and the group has no leader
- **THEN** the group reforms from the current partition map and converges on the new member set
