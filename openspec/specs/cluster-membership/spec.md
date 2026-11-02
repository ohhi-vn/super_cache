# cluster-membership Specification

## Purpose

Defines how SuperCache discovers cache nodes, assigns partition ownership, transfers data when ownership changes, and reports cluster health.

## Requirements

### Requirement: Cache-only membership

Only nodes that have completed SuperCache bootstrap SHALL be added to live membership. Connected Erlang nodes that are not running the cache MUST NOT receive partition ownership.

#### Scenario: Plain Erlang node connects

- **WHEN** an Erlang node connects to the cluster but has not completed SuperCache bootstrap
- **THEN** it does not appear in live membership and receives no primary or replica assignment

#### Scenario: Cache node completes bootstrap

- **WHEN** a connected node completes SuperCache bootstrap
- **THEN** it is added to live membership and the partition map is recomputed

### Requirement: Deterministic partition map

Given the same sorted live-node set, every node SHALL compute the same primary and replica assignment.

#### Scenario: Nodes converge on one map

- **WHEN** three cache nodes have converged on the same live-node set after joins in different orders
- **THEN** all three report identical primary and replica assignments for every partition

#### Scenario: Membership shrinks

- **WHEN** a node leaves and the remaining nodes converge on the new live-node set
- **THEN** all remaining nodes compute identical assignments for every partition

### Requirement: Node failure reassignment

When a node leaves, the system SHALL remove it from live membership, recompute the partition map, and route new writes to the new primary. Existing replicas SHALL continue serving reads.

#### Scenario: Primary leaves

- **WHEN** a partition's primary node leaves the cluster
- **THEN** new writes are routed to the newly assigned primary and primary reads return data from a surviving replica

#### Scenario: Replica leaves

- **WHEN** a non-primary replica leaves the cluster
- **THEN** the primary and remaining replicas continue serving reads and writes without changing the primary assignment

### Requirement: Single-donor ownership handoff

When a node gains a partition, exactly one deterministic donor SHALL transfer that partition. The system MUST NOT send the same partition from multiple donors.

#### Scenario: Node joins and gains ownership

- **WHEN** a joining node becomes a primary or replica for a partition it did not previously hold
- **THEN** one deterministic surviving holder transfers the partition and other holders do not send duplicate copies

#### Scenario: Duplicate handoff is suppressed

- **WHEN** two membership events trigger the same partition transfer
- **THEN** the system coalesces them into one in-flight transfer

### Requirement: Streaming handoff with accurate reporting

Partition transfer SHALL stream records in bounded batches, retry transient failures with bounded backoff, and report exact transferred and failed counts.

#### Scenario: Partition transfer succeeds

- **WHEN** a partition containing more records than one batch is transferred
- **THEN** every record is delivered and the reported transferred count equals the partition size

#### Scenario: Batch transfer fails

- **WHEN** one or more batches fail after retries
- **THEN** the transfer reports the exact number of failed records and keeps successful batches applied

### Requirement: Health checks target the owning node

Health checks SHALL evaluate the node that owns the data being checked. A check for a remote node MUST NOT write probe data to the local node's tables or attribute local statistics to the remote node.

#### Scenario: Remote node partition health

- **WHEN** the cluster health report describes a partition owned by a remote node
- **THEN** the reported record count and partition balance come from that remote node

#### Scenario: Local node health

- **WHEN** the cluster health report describes the local node
- **THEN** the reported checks come from the local node's own tables and metrics

### Requirement: Bounded health cycles

Each health cycle SHALL finish within a configured deadline. Unresponsive nodes SHALL be recorded as unhealthy or unknown and MUST NOT be silently skipped.

#### Scenario: Node times out during a health cycle

- **WHEN** a node does not respond before the health-check deadline
- **THEN** the cycle completes within the deadline and records the node as unhealthy or unknown

#### Scenario: Health cycle runs before cache bootstrap

- **WHEN** the supervision tree is running but the cache has not completed bootstrap
- **THEN** the health monitor records no health data and does not write probes

### Requirement: Unique replication-lag probes

Replication-lag probes SHALL be unique per check and SHALL be cleaned up even when the check fails or times out.

#### Scenario: Concurrent health checks

- **WHEN** two health checks measure replication lag at the same time
- **THEN** their probes do not collide and both results are attributable to the correct check

#### Scenario: Probe cleanup after timeout

- **WHEN** a replication-lag probe times out
- **THEN** the probe record is removed from every table where it was written

### Requirement: Membership observability

Membership changes, handoff failures, and health-check timeouts SHALL increment counters exposed by cluster statistics.

#### Scenario: Handoff fails

- **WHEN** a partition transfer fails after retries
- **THEN** a handoff-failure counter increases and the failure is visible in cluster statistics

#### Scenario: Health check times out

- **WHEN** a node misses the health-check deadline
- **THEN** a health-timeout counter increases and the node's status reflects the timeout

### Requirement: Bootstrap validation

Cluster bootstrap SHALL reject a replication factor below one and a non-positive partition count before creating ETS tables.

#### Scenario: Invalid replication factor

- **WHEN** a caller starts a cluster with `replication_factor: 0`
- **THEN** bootstrap raises an argument error and creates no ETS tables

#### Scenario: Invalid partition count

- **WHEN** a caller starts a cluster with `num_partition: 0`
- **THEN** bootstrap raises an argument error and creates no ETS tables
