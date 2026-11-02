# data-structures Specification

## Purpose

Defines the concurrency safety, crash recovery, consistency, and query efficiency guarantees for the higher-level `KeyValue`, `Queue`, `Stack`, and `Struct` data structures across local and distributed modes.

## Requirements

### Requirement: Bounded contention spin-waiting

Queue and Stack operations SHALL bound contention spin-waiting to a finite retry limit. They MUST NOT block a caller indefinitely when another process holds or abandons an update lock.

#### Scenario: Stack push under exhausted contention retries

- **WHEN** a stack remains locked across the maximum spin-wait retry budget during a push
- **THEN** the push returns `false` within a bounded time rather than spinning forever

#### Scenario: Stack pop or drain under exhausted contention retries

- **WHEN** a stack remains locked across the maximum spin-wait retry budget during a pop or drain
- **THEN** `pop` returns the caller's default value and `get_all` returns `[]` within a bounded time

### Requirement: Atomic queue and stack initialization

Concurrent enqueue or push operations on a newly created or recently emptied queue or stack SHALL serialize initialization and counter updates. A concurrent writer MUST NOT reset an active queue or stack counter to zero while another mutation is in flight.

#### Scenario: Concurrent pushes to a new stack

- **WHEN** multiple processes concurrently push items to a stack that does not yet exist
- **THEN** all pushed items are retained and `count` equals the number of successful pushes

#### Scenario: Concurrent enqueues to a new queue

- **WHEN** multiple processes concurrently enqueue items to a queue that does not yet exist
- **THEN** all enqueued items are retained in FIFO order and `count` equals the number of successful enqueues

#### Scenario: Empty stack pop preserves initialized state

- **WHEN** a caller pops or drains an empty stack and then pushes a new item
- **THEN** the stack remains consistent and subsequent pops return the newly pushed item

### Requirement: Crash-resilient queue and stack locks

Queue and Stack mutations SHALL release their update lock even when an unexpected error or exit occurs during the operation, and SHALL reclaim stale locks left behind by terminated or timed-out processes.

#### Scenario: Stale lock is reclaimed after holder termination

- **WHEN** a process terminates while holding a queue or stack update lock and another process performs an operation after the lock staleness window
- **THEN** the stale lock is reclaimed and the new operation succeeds without manual intervention

### Requirement: Projected KeyValue queries and counts

KeyValue `count`, `keys`, and `values` operations SHALL compute results using storage-level count and field projections rather than copying full key-value records into caller memory.

#### Scenario: Counting keys in a namespace

- **WHEN** a caller invokes `KeyValue.count` on a namespace containing multiple entries
- **THEN** the exact entry count is returned without materializing the stored values

#### Scenario: Listing keys or values in a namespace

- **WHEN** a caller invokes `KeyValue.keys` or `KeyValue.values` on a namespace
- **THEN** all keys or values for that namespace are returned without transferring unselected tuple elements

### Requirement: Batched distributed KeyValue removal

`KeyValue.remove_batch` in distributed mode SHALL remove all specified keys for the target namespace in a single routed partition operation rather than one remote call per key.

#### Scenario: Removing multiple keys from a distributed namespace

- **WHEN** a caller invokes `KeyValue.remove_batch` with a list of keys in distributed mode
- **THEN** all listed keys are removed from the primary and replicas in one partition batch operation

### Requirement: Primary-consistent Struct removal and fast metadata lookup

`Struct` operations SHALL cache initialized struct key-field metadata for fast repeated lookups (clearing cached metadata on cache stop) and SHALL execute distributed `Struct.remove` directly on the primary partition owner without pre-filtering against a potentially stale local replica.

#### Scenario: Removing a struct when the local replica is stale

- **WHEN** a struct exists on the partition primary in distributed mode and a caller invokes `Struct.remove` from a node whose local replica has not yet received the struct
- **THEN** the struct is removed on the primary and replicas and `Struct.remove` returns `{:ok, removed_struct}`

#### Scenario: Removing a non-existent struct in distributed mode

- **WHEN** a caller invokes `Struct.remove` for a struct ID that does not exist on the primary
- **THEN** `Struct.remove` returns `{:error, :not_found}`
