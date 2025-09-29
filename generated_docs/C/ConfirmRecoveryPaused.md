# ConfirmRecoveryPaused

## Location
[src/backend/access/transam/xlogrecovery.c:3110-3130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L3110-L3130)

## Overview
Confirms a pending recovery pause request by transitioning the state from RECOVERY_PAUSE_REQUESTED to RECOVERY_PAUSED.

## Definition
static void ConfirmRecoveryPaused(void)

## Detailed Description
This function implements the confirmation phase of the two-phase recovery pause mechanism. It checks if a pause has been requested (RECOVERY_PAUSE_REQUESTED state) and if so, atomically updates the state to RECOVERY_PAUSED to indicate that the recovery process has actually paused.

The function uses spinlock protection to ensure thread-safe access to the shared recovery pause state. It only performs the state transition if the current state is RECOVERY_PAUSE_REQUESTED, making it safe to call repeatedly without unwanted side effects.

This design separates the pause request (handled by SetRecoveryPause) from the pause confirmation, allowing the recovery process to acknowledge the pause at appropriate safe points during WAL replay.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire  
  - SpinLockRelease
  - RECOVERY_PAUSE_REQUESTED
  - RECOVERY_PAUSED
  - XLogRecoveryCtl (global structure)
- Called from (representative examples):
  - [recoveryPausesHere](../r/recoveryPausesHere.md)
  - [RecoveryRequiresIntParameter](../R/RecoveryRequiresIntParameter.md)

## Notes and Other Information
- This is a static function within xlogrecovery.c, not exposed as a public API
- Part of the two-phase pause mechanism: request (via SetRecoveryPause) followed by confirmation (via this function)
- Thread-safe implementation using spinlocks for atomic state updates
- Only transitions from RECOVERY_PAUSE_REQUESTED to RECOVERY_PAUSED, ignoring other states
- Called at safe points during recovery where pausing is appropriate
- Location: src/backend/access/transam/xlogrecovery.c:3110-3130

## Simplified Source

```c
static void ConfirmRecoveryPaused(void)
{
    // Atomically update recovery pause state
    SpinLockAcquire(&XLogRecoveryCtl->info_lck);

    // Only confirm if pause was requested
    if (XLogRecoveryCtl->recoveryPauseState == RECOVERY_PAUSE_REQUESTED)
        XLogRecoveryCtl->recoveryPauseState = RECOVERY_PAUSED;

    SpinLockRelease(&XLogRecoveryCtl->info_lck);
}
```