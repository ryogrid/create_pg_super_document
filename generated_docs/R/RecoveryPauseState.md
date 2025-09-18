# RecoveryPauseState

## Location
[src/include/access/xlogrecovery.h:49-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlogrecovery.h#L49-L131)

## Overview
An enumeration that defines the different states of recovery pause functionality in PostgreSQL's Write-Ahead Log (WAL) recovery process.

## Definition


## Detailed Description
The RecoveryPauseState enum represents the three possible states during WAL recovery pause operations. This mechanism allows administrators to temporarily halt recovery processing for maintenance, debugging, or other operational purposes. The enum provides a state machine with three distinct phases: normal operation, pause transition, and paused state.

Recovery pause is controlled through PostgreSQL's recovery control system and is used internally to manage the transition between active recovery and paused states safely with proper synchronization.

## Parameters / Member Variables
- : Normal recovery state where WAL replay continues without interruption
- : Intermediate state indicating a pause has been requested but recovery hasn't yet reached the paused state
- : Recovery is fully paused and WAL replay is halted

## Dependencies
- Functions called/Symbols referenced:
  - (This is an enum type with no function calls)
- Called from (representative examples):
  - [GetRecoveryPauseState](../G/GetRecoveryPauseState.md) (src/backend/access/transam/xlogrecovery.c:3072)
  - [XLogRecoveryCtlData](../X/XLogRecoveryCtlData.md).recoveryPauseState (src/backend/access/transam/xlogrecovery.c:358)
  - SetRecoveryPauseState functions
  - Recovery pause management functions in xlogfuncs.c

## Notes and Other Information
- Used as a member variable in XLogRecoveryCtlData structure for tracking global recovery pause state
- State transitions are protected by spinlocks to ensure thread-safe access
- The intermediate RECOVERY_PAUSE_REQUESTED state allows for graceful pause transitions
- Used in conjunction with condition variables (recoveryNotPausedCV) for proper synchronization during pause/resume operations
- State management is primarily handled in src/backend/access/transam/xlogrecovery.c