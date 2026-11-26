# Spec Delta

## ADDED Requirements

### Requirement: Cluster sizing diagnostics (N+1)

When raft mode is active, the system SHALL compare the live node count with `replication_factor + 1` on every membership change. When the topology is below that threshold it MUST expose an `underprovisioned` status and log a warning on the transition into that state; at or above it MUST expose a healthy status. The comparison MUST NOT block startup or writes.

#### Scenario: Under-provisioned cluster still starts

- **WHEN** two nodes start with `replication_mode: :raft` and `replication_factor: 2`
- **THEN** both nodes bootstrap, log that three nodes are recommended, and report `underprovisioned` in cluster statistics and local health
- **AND** writes still commit while the partition's Raft majority is available

#### Scenario: Status clears when a spare joins

- **WHEN** a third node joins the same cluster
- **THEN** the membership-change evaluation marks the topology healthy and the warning status clears on every node

#### Scenario: Single-node development remains valid

- **WHEN** one node runs raft mode with `replication_factor: 1` or with a replication factor larger than the cluster
- **THEN** startup succeeds and the topology status reflects the configured recommendation without rejecting the node

### Requirement: Supported raft topologies

Raft mode SHALL support a single node, a cluster smaller than the replication factor, a cluster equal to it, and a cluster with one or more spare nodes. Each partition's voting group MUST contain `min(replication_factor, live_node_count)` members derived from the deterministic partition map.

#### Scenario: Single-node group

- **WHEN** a node runs raft mode alone
- **THEN** every partition group has one member and committed writes succeed

#### Scenario: Replication factor exceeds cluster size

- **WHEN** two nodes run with `replication_factor: 3`
- **THEN** every partition group contains both nodes and a write succeeds only while both are available

#### Scenario: N+1 nodes with a spare

- **WHEN** three nodes run with `replication_factor: 2`
- **THEN** each partition group contains two members and the third node remains available as a replacement

### Requirement: Leader failover

When a group loses its leader while a majority remains, the system SHALL elect a new leader and resume commits without operator action. A write that fails only because leadership moved SHALL be retried within the command deadline; a write whose commit outcome is unknown MUST NOT be retried.

#### Scenario: Leader stops and a new leader takes over

- **WHEN** the leader of a three-member group stops while two members remain
- **THEN** a new leader is elected and a subsequent write commits successfully

#### Scenario: Transient leader placement failure

- **WHEN** a write reaches a follower or a group without a leader and a majority is reachable
- **THEN** the write is redirected or retried until it commits or the configured deadline expires

#### Scenario: No majority available

- **WHEN** no leader can be elected because a majority of the group is unavailable
- **THEN** the write fails within the configured deadline instead of blocking indefinitely

### Requirement: Spare-member replacement and catch-up

When a member is lost and the cluster has a spare node, the partition's group SHALL replace the lost member with the spare and resume commits once a majority exists. A joining member MUST rebuild its partition from the committed log and MUST NOT accept a direct ETS handoff for a Raft-managed partition.

#### Scenario: Failed member is replaced by the spare

- **WHEN** a member of a partition group stops and a spare node exists
- **THEN** the group is reconciled to include the spare, previously committed data remains readable, and writes resume

#### Scenario: Joining member replays historical increments

- **WHEN** a member joins a group whose log contains historical `increment` commands
- **THEN** the member's final counter equals the leader's counter and historical increments are applied exactly once

#### Scenario: Rejoining member state is rebuilt

- **WHEN** a node that previously held a partition rejoins the group
- **THEN** its local partition, applied-index watermark, and persisted Raft server state are reset before it rejoins
- **AND** its state is rebuilt from the committed log so the log is the source of truth

### Requirement: Safe state-machine application

A committed command MUST be reported as successful only when the state machine applied it or confirmed it was already applied. The applied-index watermark MUST advance only after the command's state changes are complete, and replay of an index at or below the watermark MUST be acknowledged without re-applying its operations.

#### Scenario: Apply failure is not reported as success

- **WHEN** a committed command cannot be applied to the local partition
- **THEN** the caller receives a documented write failure instead of a success value

#### Scenario: Watermark does not advance on failure

- **WHEN** applying a command fails before its watermark is recorded
- **THEN** a later retry of the same index applies the command instead of skipping it as a replay

#### Scenario: Replay of an applied increment

- **WHEN** an increment index at or below the stored watermark is replayed
- **THEN** the command is acknowledged without changing the counter

## MODIFIED Requirements

### Requirement: Leader routing

A write submitted to a node that is not the partition's Raft leader SHALL be routed to the leader without changing the caller's result shape. If no leader can be reached within the configured timeout, the write MUST fail.

#### Scenario: Write submitted to a follower

- **WHEN** a caller sends a raft-mode write to a node that does not lead the partition's Raft group
- **THEN** the write is forwarded and returns the same success shape as a write sent to the leader

#### Scenario: No reachable leader

- **WHEN** no leader for the partition can be reached within the timeout
- **THEN** the write returns a failure instead of blocking indefinitely

#### Scenario: Leadership moves during the write

- **WHEN** a write fails with a leader-placement error and a majority is still reachable
- **THEN** the write is retried against the new leader within the same deadline

#### Scenario: Ambiguous timeout is not retried

- **WHEN** a write times out after it may have been committed
- **THEN** the system returns a failure without automatically resubmitting the command

### Requirement: Deterministic partition-scoped groups

Each partition SHALL be replicated by one independent Raft group whose members are derived deterministically from the partition's primary and replicas. Nodes sharing the same partition map MUST agree on every group's membership, and membership changes MUST update the groups consistently.

#### Scenario: Nodes agree on group membership

- **WHEN** two nodes have the same partition map
- **THEN** they assign the same members to each partition's Raft group

#### Scenario: Membership changes

- **WHEN** a node joins or leaves the cache cluster
- **THEN** affected Raft groups are updated to reflect the new member set without diverging between nodes

#### Scenario: Primary moves between nodes

- **WHEN** a partition's primary changes to another node
- **THEN** the old primary stops its local group for that partition and only the new primary performs membership reconciliation

#### Scenario: New primary catches up after ownership moves

- **WHEN** a partition's primary leaves and a surviving member becomes primary
- **THEN** the new primary forms the group with the new member set and catches up from the committed log
- **AND** previously committed data converges on every remaining group member

### Requirement: Bootstrap lifecycle and recovery

Raft groups SHALL start only after the cache has bootstrapped and SHALL be released when the cache stops. After a restart, committed log entries MUST be recovered and applied idempotently so replay does not duplicate effects.

#### Scenario: Restart recovers committed entries

- **WHEN** a node restarts under raft mode with entries still present in its Raft log
- **THEN** those entries are recovered and the final partition state matches the committed state

#### Scenario: Cache stops

- **WHEN** the cache is stopped under raft mode
- **THEN** Raft groups are shut down and their resources are released

#### Scenario: Whole cluster restarts

- **WHEN** every node in a raft-mode cluster stops and starts again with its persisted Raft data directory
- **THEN** committed entries are recovered on every member and writes resume once a majority of each group is running

#### Scenario: ETS partitions are recreated

- **WHEN** storage tables are dropped and recreated while Raft data is retained
- **THEN** the applied-index watermarks are cleared so committed entries are applied again to the new tables

### Requirement: Raft observability

Raft mode SHALL expose its operational state through existing cluster statistics and health reporting. Write failures, leadership changes, and group status MUST be visible to operators.

#### Scenario: Raft stats are available

- **WHEN** an operator queries cluster statistics on a node in raft mode
- **THEN** the report includes Raft group status and write counters

#### Scenario: Raft failure is counted

- **WHEN** a Raft-mode write fails
- **THEN** a Raft failure counter increases and is visible in cluster statistics

#### Scenario: Topology status is reported

- **WHEN** an operator queries cluster statistics or local health in raft mode
- **THEN** the report includes the live node count, the recommended node count, and whether the cluster is under-provisioned

#### Scenario: Group details are reported

- **WHEN** an operator queries cluster statistics in raft mode
- **THEN** each locally tracked group reports its partition, leadership role, leader when known, and current member count
- **AND** the existing `raft.groups` list of partition indexes remains available
