# Spec Delta

## Purpose

Defines the distributed replication acknowledgement deduplication, write/read failure propagation, replica-aware primary failover, node-local health verification, and single-donor streaming handoff guarantees.

## ADDED Requirements

### Requirement: Per-replica WAL acknowledgement deduplication
Strong-consistency WAL commits SHALL count at most one acknowledgement per distinct replica node for a given sequence number. Duplicate acknowledgements from the same replica MUST NOT advance the quorum count.

#### Scenario: Duplicate acknowledgement from one replica
- **WHEN** a partition has one primary and two replicas and one replica sends two acknowledgements for the same sequence number while the second replica has not acknowledged
- **THEN** the second acknowledgement from the same replica is ignored and the commit waits until a true majority of distinct nodes has acknowledged

### Requirement: Distributed write failure propagation
Sync and strong distributed writes SHALL propagate quorum and transport failures to the caller. Bang write APIs SHALL raise `SuperCache.Cluster.Error` and non-bang write APIs SHALL return `{:error, %SuperCache.Cluster.Error{}}` rather than reporting success.

#### Scenario: Sync or strong write fails to reach quorum
- **WHEN** a sync or strong write cannot reach the required replica quorum
- **THEN** bang write functions raise `SuperCache.Cluster.Error` and non-bang write functions return `{:error, %SuperCache.Cluster.Error{}}`

#### Scenario: Forwarded write to an unreachable primary
- **WHEN** a write is routed from a non-primary node to an unreachable primary node
- **THEN** the caller receives `SuperCache.Cluster.Error` with operation, partition, and node context

### Requirement: Replica-aware primary read failover
When a `:primary` or `:quorum` read cannot reach the primary or form a quorum, the system SHALL fall back to a local read only when the local node holds a replica for that partition, and SHALL otherwise raise or return `SuperCache.Cluster.Error` instead of returning an empty result.

#### Scenario: Primary read fails on a node holding a replica
- **WHEN** a primary read cannot reach the primary node and the local node is a replica for the partition
- **THEN** the read returns the local replica's data and increments the read-fallback metric

#### Scenario: Primary read fails on a node without a replica
- **WHEN** a primary read cannot reach the primary node and the local node does not hold a replica for the partition
- **THEN** bang read APIs raise `SuperCache.Cluster.Error` and non-bang read APIs return `{:error, %SuperCache.Cluster.Error{}}`

### Requirement: Node-local health evaluation and unique probe cleanup
Each node's health monitor SHALL evaluate partition counts and replication lag only for partitions owned by that node, aggregate cluster health via bounded remote queries, and use unique replication probes that are cleaned up on both primary and replicas even on timeout.

#### Scenario: Cluster health aggregates remote node checks
- **WHEN** `cluster_health` is queried on a multi-node cluster
- **THEN** each node's partition and error-rate checks reflect that node's own data, and unreachable peers are reported as `:unhealthy` or `:degraded` using cached state

#### Scenario: Replication lag probe cleanup on timeout
- **WHEN** a replication-lag probe times out before the replica observes it
- **THEN** the unique probe record is deleted from the primary and replica tables

### Requirement: Single-donor streaming partition handoff
When cluster membership changes, at most one deterministic surviving holder SHALL transfer each affected partition to the new holder using bounded streaming batches with retry and exact transferred/failed record accounting.

#### Scenario: Multiple surviving holders during node join
- **WHEN** a new node joins a cluster where both a primary and a replica already hold a partition
- **THEN** only the deterministic donor transfers the partition and duplicate concurrent transfers for the same `{partition, target}` pair are coalesced
