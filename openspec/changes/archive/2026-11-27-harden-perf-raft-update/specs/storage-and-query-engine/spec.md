# Spec Delta

## ADDED Requirements

### Requirement: Efficient count and field projections

`KeyValue.count/2`, `keys/2`, and `values/2` SHALL compute results with storage-level counting and field projections instead of copying full records into the caller.

#### Scenario: Count does not materialize records

- **WHEN** a caller counts keys in a large namespace
- **THEN** the result is computed without copying every record into the caller

#### Scenario: Keys and values use projections

- **WHEN** a caller lists keys or values for a namespace
- **THEN** only the requested fields are returned and full records are not materialized

### Requirement: Cached struct metadata with invalidation

Struct key-field metadata resolved once SHALL be cached for repeated struct operations and SHALL be invalidated when the struct table stops so stale metadata is never reused.

#### Scenario: Repeated struct operations reuse cache

- **WHEN** a caller performs repeated struct operations on the same table
- **THEN** key-field resolution does not repeat storage lookups for every operation

#### Scenario: Cache cleared on stop

- **WHEN** the struct table stops
- **THEN** cached metadata is cleared and subsequent operations re-resolve it
