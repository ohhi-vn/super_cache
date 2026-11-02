# Spec Delta

## Purpose

Defines the ordering, batching, and fault-isolation behavior of the per-scheduler `lazy_put` write buffer pipeline.

## ADDED Requirements

### Requirement: FIFO ordering within a scheduler buffer
Items enqueued into the same scheduler buffer via `lazy_put` SHALL be delivered to the cache in first-in, first-out (FIFO) order so later updates to the same key are never overwritten by earlier updates in the same batch.

#### Scenario: Sequential lazy writes to the same key on one buffer
- **WHEN** a process enqueues `{:user, 1, :v1}` followed by `{:user, 1, :v2}` into the same buffer before the stream drains
- **THEN** the buffer flushes `:v1` before `:v2` and the final cached record is `{:user, 1, :v2}`

### Requirement: Batched buffer flushing
When a buffer stream drains multiple queued items in a single cycle, it SHALL flush valid tuple batches using batch cache writes rather than issuing an individual write per item, while isolating malformed items so a single invalid entry does not drop the rest of the batch.

#### Scenario: Draining a multi-item buffer batch
- **WHEN** multiple tuples accumulate in a buffer queue before the consumer stream reads them
- **THEN** all valid tuples in the drained batch are written to the cache and become readable

#### Scenario: Malformed item in a batch does not abort valid items
- **WHEN** a drained buffer batch contains both valid tuples and a tuple that fails key/partition extraction
- **THEN** the valid tuples in the batch are still written to the cache and the error for the invalid item is logged
