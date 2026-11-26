# raft-snapshot-recovery Specification

## Purpose

Defines snapshot checkpointing and recovery for Raft-managed partitions so the applied-index watermark survives restarts while replay after ETS recreation remains idempotent.

## Requirements

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

### Requirement: Snapshot recovery restores the watermark

On startup or log recovery from a snapshot, the system SHALL restore the applied-index watermark from persisted snapshot state so entries already applied before the restart are acknowledged without being applied twice.

#### Scenario: Restart with a snapshot

- **WHEN** a node restarts with a Raft snapshot present for a partition
- **THEN** the applied watermark reflects the snapshotted index and replayed entries at or below it are skipped

#### Scenario: Counter replay after restart

- **WHEN** a partition's log contains increment entries that were applied before a restart
- **THEN** replay after the restart does not increment those entries a second time

### Requirement: ETS recreation still replays committed entries

When storage tables are dropped and recreated while Raft data is retained, the system SHALL treat the empty tables as the authoritative state and re-apply committed entries even if a snapshot records a higher applied index. Restoring a watermark MUST NOT suppress rebuilding recreated partitions.

#### Scenario: Tables recreated with retained log

- **WHEN** ETS partitions are recreated while the Raft snapshot and log are retained
- **THEN** committed entries are applied to the new tables and the final partition state matches the committed state

#### Scenario: Watermark re-established after replay

- **WHEN** replay finishes after table recreation
- **THEN** the applied watermark again matches the highest successfully applied index