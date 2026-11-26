# Spec Delta

## ADDED Requirements

### Requirement: Role-aware Raft membership

Every member of a Raft partition group SHALL have a membership role of voter, promotable, or non-voter. A joining cache node SHALL be added to the group as a promotable member and promoted to voter only after it has caught up with the committed log, so the group converges to the configured voting set. A departing member SHALL be demoted before it is removed, and the system MUST NOT demote or remove the last remaining voter of a group.

#### Scenario: Joining member is promoted after catch-up

- **WHEN** a node joins a partition's Raft group and replays the committed log
- **THEN** it joins as a promotable member and becomes a voter once it is caught up

#### Scenario: Departing member is demoted before removal

- **WHEN** a node leaves the cache cluster and a partition group must drop it
- **THEN** the member is demoted before removal and the remaining voters keep a working majority

#### Scenario: Last voter is protected

- **WHEN** a membership change would leave a group with no voters
- **THEN** the change is rejected and the existing voter set is preserved
