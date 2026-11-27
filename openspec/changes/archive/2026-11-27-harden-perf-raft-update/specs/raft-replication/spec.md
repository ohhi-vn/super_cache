# Spec Delta

## MODIFIED Requirements

### Requirement: Raft failure normalization

Failures specific to Raft — quorum timeouts, lost leadership, transport errors, unreachable members, durable-write failures, and bounded-queue backpressure — SHALL be normalized to the documented distributed error. Bang APIs MUST raise it and non-bang APIs MUST return `{:error, exception}`; raw exits and transport tuples MUST NOT escape to callers.

#### Scenario: Raft quorum times out

- **WHEN** a raft-mode write times out waiting for a quorum
- **THEN** the bang API raises the documented error and the non-bang API returns an error tuple

#### Scenario: Remote node is unreachable

- **WHEN** a Raft peer is disconnected during a write
- **THEN** the caller receives the documented error rather than a raw exit

#### Scenario: Durable-write failure is reported

- **WHEN** the Raft engine reports a durable-write failure instead of success
- **THEN** the caller receives the documented error and the write is not reported as committed

#### Scenario: Backpressure is reported

- **WHEN** a raft-mode write is rejected because the pending-command bound is reached
- **THEN** the caller receives a documented backpressure error through the same bang/non-bang contract

### Requirement: Raft observability

Raft mode SHALL expose its operational state through existing cluster statistics and health reporting. Write failures, leadership changes, group status, replication lag, and effective tuning MUST be visible to operators.

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

#### Scenario: Engine metrics are reported

- **WHEN** an operator queries cluster statistics in raft mode
- **THEN** each local group reports its term, commit index, applied index, and Raft state when the engine provides them

#### Scenario: Member details are reported

- **WHEN** an operator queries group details in raft mode
- **THEN** reported members include their membership role and voter status when the engine provides them

#### Scenario: Replication lag is reported

- **WHEN** the engine provides per-server replication-lag gauges
- **THEN** group details include the lag so operators can see catch-up progress

### Requirement: Raft engine configuration

Raft mode SHALL validate Raft-specific configuration before starting any group and SHALL apply the accepted values to every group it forms. Configuration that cannot produce a working group MUST be rejected at startup rather than on the first write.

#### Scenario: Valid configuration is applied

- **WHEN** a node starts in raft mode with valid election, heartbeat, WAL, snapshot, throughput, compaction, and segment settings
- **THEN** those settings are applied to the groups the node forms and startup succeeds

#### Scenario: Invalid configuration is rejected

- **WHEN** a node starts in raft mode with an invalid Raft setting
- **THEN** bootstrap fails with a configuration error and no Raft group is started
