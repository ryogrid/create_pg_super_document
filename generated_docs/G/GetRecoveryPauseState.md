# GetRecoveryPauseState

## Location
src/backend/access/transam/xlogrecovery.c: 3070 - 3089

## Overview
Returns the current state of the recovery pause mechanism, providing thread-safe access to the shared recovery pause state.

## Definition
RecoveryPauseState GetRecoveryPauseState(void)

## Detailed Description
This function provides a thread-safe way to query the current recovery pause state from shared memory. It uses a spinlock to ensure atomic access to the recoveryPauseState field in the XLogRecoveryCtl structure. The function is essential for determining whether recovery is currently paused, requested to pause, or running normally.

The recovery pause state can have different values representing various stages of the pause mechanism:
- RECOVERY_NOT_PAUSED: Recovery is running normally
- RECOVERY_PAUSE_REQUESTED: A pause has been requested but not yet confirmed  
- RECOVERY_PAUSED: Recovery is currently paused

This function serves as the primary interface for checking the pause state from various parts of the system, including SQL functions and internal recovery logic.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - RecoveryPauseState (return type)
  - SpinLockAcquire
  - SpinLockRelease
  - XLogRecoveryCtl (global structure)
- Called from (representative examples):
  - pg_is_wal_replay_paused
  - pg_get_wal_replay_pause_state  
  - recoveryPausesHere
  - RecoveryRequiresIntParameter
  - EndOfWalRecoveryInfo

## Notes and Other Information
- This function is thread-safe due to spinlock protection around the shared state access
- Used by both internal recovery processes and SQL-accessible functions for pause state inspection
- The spinlock ensures that the state read is atomic and consistent
- Part of the public interface for recovery pause functionality
- Location: src/backend/access/transam/xlogrecovery.c:3070-3089