# SetRecoveryPause

## Location
src/backend/access/transam/xlogrecovery.c: 3090 - 3109

## Overview
Sets the recovery pause state in shared memory, either requesting a pause or resuming recovery, with proper synchronization and notification mechanisms.

## Definition
void SetRecoveryPause(bool recoveryPause)

## Detailed Description
This function provides the primary interface for controlling the recovery pause mechanism. It safely updates the shared recovery pause state using spinlock protection and implements a state transition logic that prevents invalid state changes.

When requesting a pause (recoveryPause = true):
- Only transitions from RECOVERY_NOT_PAUSED to RECOVERY_PAUSE_REQUESTED
- Does not change state if already paused or pause requested

When requesting resume (recoveryPause = false):  
- Immediately sets state to RECOVERY_NOT_PAUSED regardless of current state
- Broadcasts on the condition variable to wake up any waiting recovery processes

The function implements a two-phase pause mechanism where requesting a pause only sets the request flag, and the actual pause confirmation happens later via ConfirmRecoveryPaused().

## Parameters / Member Variables
- : Boolean indicating whether to request pause (true) or resume recovery (false)

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - RECOVERY_NOT_PAUSED
  - RECOVERY_PAUSE_REQUESTED
  - ConditionVariableBroadcast
  - XLogRecoveryCtl (global structure)
- Called from (representative examples):
  - [pg_wal_replay_pause](../p/pg_wal_replay_pause.md)
  - [pg_wal_replay_resume](../p/pg_wal_replay_resume.md)
  - [PerformWalRecovery](../P/PerformWalRecovery.md)
  - [SetPromoteIsTriggered](SetPromoteIsTriggered.md)
  - [RecoveryRequiresIntParameter](../R/RecoveryRequiresIntParameter.md)

## Notes and Other Information
- Thread-safe implementation using spinlocks for atomic state updates
- Uses condition variable broadcast to efficiently notify waiting processes when recovery resumes
- Part of the public interface for recovery pause control, accessible through SQL functions
- The pause request is asynchronous - actual pause confirmation happens separately
- Resume operations take precedence and immediately set the state to not paused
- Location: src/backend/access/transam/xlogrecovery.c:3090-3109