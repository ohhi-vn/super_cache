# Spec Delta

## Purpose

Defines the storage engine's pattern-matching semantics, atomic batch write execution, and supervised table ownership for read-your-writes tracking.

## ADDED Requirements

### Requirement: Nested and ordered match-pattern binding extraction
Pattern-match queries (`get_by_match`) SHALL extract and return all match variables (`:"$1"`, `:"$2"`, ...) regardless of tuple nesting depth, ordered numerically by variable index.

#### Scenario: Nested tuple pattern captures bindings
- **WHEN** a caller queries `get_by_match` with a pattern containing capture variables inside nested tuples (such as `{{:ns, :"$1"}, :"$2"}`)
- **THEN** each matching record returns `[binding_1, binding_2]` in numerical variable order

#### Scenario: Out-of-order capture variables are sorted by variable number
- **WHEN** a caller queries `get_by_match` with `{:record, :"$2", :"$1"}`
- **THEN** each matching record returns `[value_of_1, value_of_2]` matching `:ets.match/2` semantics

### Requirement: Single-operation partition batch writes
When applying a batch of `put` operations to a partition locally or on a replica, the system SHALL insert the entire partition batch in a single storage write rather than iterating record by record.

#### Scenario: Multi-record batch write to a single partition
- **WHEN** a caller writes a batch of tuples that map to the same partition
- **THEN** all tuples for that partition are inserted in a single storage operation on both the primary and replicas

### Requirement: Supervised read-your-writes table ownership
The read-your-writes tracking table SHALL be owned by a permanent supervised process rather than created lazily by the first calling process.

#### Scenario: Short-lived writer process exits after a distributed write
- **WHEN** a short-lived spawned process performs a distributed write and immediately terminates
- **THEN** the read-your-writes tracking table remains alive and subsequent writers and readers do not fail or lose tracking state

#### Scenario: Concurrent initial writes do not race on table creation
- **WHEN** multiple processes perform the first distributed writes concurrently
- **THEN** all writes succeed without raising a table-creation conflict error

### Requirement: Match-spec pruning for expired read-your-writes entries
Expired read-your-writes tracking entries SHALL be pruned using a storage match-delete specification without scanning live entries into caller memory.

#### Scenario: Expired read-your-writes entries are pruned
- **WHEN** read-your-writes entries exceed their TTL and pruning is triggered
- **THEN** expired entries are removed while active non-expired entries remain intact
