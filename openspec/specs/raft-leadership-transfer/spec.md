# raft-leadership-transfer Specification

## Purpose

Defines graceful leadership handoff for Raft partition groups so a planned primary or ownership move transfers leadership to the incoming node instead of waiting for an election timeout.

## Requirements

### Requirement: Graceful transfer on planned primary move

When a partition's primary changes to another node and the outgoing node currently leads that partition's Raft group, the system SHALL transfer leadership to an up-to-date target voter before tearing down the outgoing server. The transfer MUST target a member that is caught up with the committed log.

#### Scenario: Planned primary move with a caught-up target

- **WHEN** a partition's primary moves to a surviving member that is current with the committed log
- **THEN** leadership moves to that member and subsequent writes commit without waiting for an election timeout

#### Scenario: Transfer targets only a voter

- **WHEN** a planned move is evaluated and the intended target is not a voter
- **THEN** the system does not transfer leadership to it and keeps the existing leader until a suitable voter exists

### Requirement: Leadership transfer failure fallback

If a leadership transfer cannot complete because the target is unavailable, not caught up, or the request times out, the system SHALL fall back to the normal group reconciliation and election path. The fallback MUST NOT report the write outcome as successful, and the failed transfer MUST be visible in cluster statistics.

#### Scenario: Transfer times out

- **WHEN** a leadership transfer request does not complete within its deadline
- **THEN** the group continues under its current leader and reconciliation proceeds without a claimed successful transfer

#### Scenario: New leader takes over after fallback

- **WHEN** a transfer fails and the old leader is subsequently stopped while a majority remains
- **THEN** a new leader is elected and later writes commit normally

### Requirement: No transfer on unplanned loss

Leadership transfer SHALL be used only for planned ownership changes. An abrupt leader failure MUST rely on Raft election rather than an explicit transfer request.

#### Scenario: Leader stops unexpectedly

- **WHEN** a partition's leader node fails without a planned ownership change
- **THEN** the remaining voters elect a new leader through the normal election path