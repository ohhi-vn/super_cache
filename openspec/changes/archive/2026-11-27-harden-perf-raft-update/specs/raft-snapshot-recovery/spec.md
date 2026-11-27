# Spec Delta

## MODIFIED Requirements

### Requirement: Applied-index checkpointing

The system SHALL checkpoint the highest applied log index into Raft snapshot metadata as committed entries are applied, so replay can recognize entries that were already applied. Because local partitions are rebuilt from the committed log on startup, log compaction MUST NOT discard entries that are still required to reconstruct a partition. A checkpoint MUST NOT advance past an index whose state-machine application has completed.

#### Scenario: Applied entries are checkpointed

- **WHEN** a committed entry is applied to a partition
- **THEN** the applied index is included in snapshot metadata and a replay of that index is acknowledged without re-applying it

#### Scenario: Compaction preserves replay data

- **WHEN** a snapshot checkpoint is written for a partition
- **THEN** log entries needed to rebuild that partition's ETS contents from the committed log remain available for replay

#### Scenario: Failed application is not checkpointed

- **WHEN** applying a committed entry fails
- **THEN** the applied index checkpoint does not advance past that entry

#### Scenario: Automatic compaction preserves replay data

- **WHEN** automatic compaction triggers on entry-count, WAL-bytes, or time thresholds
- **THEN** entries still required to rebuild the partition remain available and committed state still converges after restart

## ADDED Requirements

### Requirement: No log-discarding catch-up repair

The system SHALL NOT discard committed Raft log entries as part of catch-up
repair. When a joining member fails to catch up while the group holds
committed data, reconciliation MUST report a failure instead of rebuilding
the group from scratch with an empty log.

#### Scenario: Repair preserves committed data

- **WHEN** a joining member fails to catch up while its log contains committed writes
- **THEN** reconciliation reports a failure and no committed entry is lost

#### Scenario: Departure reforms from survivors

- **WHEN** the leader departs and no leader exists for the group
- **THEN** the group reforms from the current partition map and previously committed data remains readable on the surviving members

#### Scenario: Snapshot transfer is used for catch-up

- **WHEN** a joining member is behind beyond retained log entries
- **THEN** the member catches up via snapshot transfer and converges to the leader's state
