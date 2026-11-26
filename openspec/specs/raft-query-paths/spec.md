# raft-query-paths Specification

## Purpose

Defines how reads of Raft-managed partitions use Raft-native query paths so callers can choose local speed, leader freshness, or quorum-confirmed consistency without changing existing read APIs.

## Requirements

### Requirement: Raft-native query path selection

For a partition replicated in Raft mode, the system SHALL serve each read mode through the matching Raft query path: a local-mode read through the local server state, a primary-mode read through the leader, and an explicit consistent-mode read through a quorum-confirmed query. This selection MUST NOT change the documented success return shape of any existing read mode.

#### Scenario: Local read uses local state

- **WHEN** a caller performs a `:local` read on a Raft-managed partition
- **THEN** the read is served from the local server state without contacting the leader and returns the normal read result

#### Scenario: Primary read uses the leader

- **WHEN** a caller performs a `:primary` read on a Raft-managed partition and the partition has a reachable leader
- **THEN** the read is served by the leader and returns the normal read result

#### Scenario: Consistent read uses quorum confirmation

- **WHEN** a caller selects the consistent read mode for a Raft-managed partition
- **THEN** the read confirms leadership with a voter quorum before returning the value

### Requirement: Consistent read failure contract

A consistent read that cannot confirm leadership with a voter quorum SHALL fail rather than return a possibly stale value. Bang read APIs MUST raise the documented distributed error and non-bang read APIs MUST return `{:error, exception}`; no raw transport tuple or exit signal MAY escape to the caller.

#### Scenario: Quorum unavailable for consistent read

- **WHEN** a consistent read cannot reach a majority of voters within the configured timeout
- **THEN** the read reports a documented failure instead of returning a local value

#### Scenario: Consistent read failure is observable

- **WHEN** a consistent read fails because leadership cannot be confirmed
- **THEN** the failure increments the Raft failure counter exposed in cluster statistics

### Requirement: Existing read modes are preserved

Enabling Raft mode SHALL NOT require callers to change existing read calls. The new consistent mode MUST be opt-in, and enabling it on one call MUST NOT alter the behavior of later `:local` or `:primary` reads.

#### Scenario: Existing call is unchanged

- **WHEN** an application keeps using its existing `:local` or `:primary` read calls after the Raft upgrade
- **THEN** those calls keep their previous success and failure shapes

#### Scenario: Consistent mode is scoped to the call

- **WHEN** one read selects the consistent mode and a subsequent read omits it
- **THEN** the subsequent read uses its usual Raft query path