# recoveryPausesHere

## Location
[src/backend/access/transam/xlogrecovery.c:2925-2981](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L2925-L2981)

## Overview
Pauses WAL recovery and waits until the shared recoveryPauseState is set to RECOVERY_NOT_PAUSED, allowing administrators to inspect the database state during recovery.

## Definition

```c
static void
recoveryPausesHere(bool endOfRecovery)
```
## Detailed Description
This function implements the core pause mechanism during PostgreSQL WAL recovery. It enters a waiting loop that continues until recovery is explicitly resumed by a user or administrator. The function provides different behavior and messaging depending on whether the pause occurs at the end of recovery (when recovery targets are reached) or during intermediate recovery steps.

The function performs several safety checks before pausing:
- Only pauses when users can connect (LocalHotStandbyActive is true)
- Skips pausing if standby promotion has been triggered
- Provides appropriate log messages with hints for resuming recovery

During the pause loop, it periodically checks for interrupts and standby triggers while confirming the paused state. The implementation uses a condition variable with timeout for efficient waiting.

## Parameters / Member Variables
- `endOfRecovery`: Boolean indicating whether the pause occurs at the end of recovery due to recovery_target_action=pause (true) or during intermediate recovery (false)
## Dependencies
- Functions called/Symbols referenced:
  - [GetRecoveryPauseState](../G/GetRecoveryPauseState.md)
  - RECOVERY_NOT_PAUSED
  - [HandleStartupProcInterrupts](../H/HandleStartupProcInterrupts.md)
  - [CheckForStandbyTrigger](../C/CheckForStandbyTrigger.md)
  - [ConfirmRecoveryPaused](../C/ConfirmRecoveryPaused.md)
  - [ConditionVariableTimedSleep](../C/ConditionVariableTimedSleep.md)
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md)
- Called from (representative examples):
  - [PerformWalRecovery](../P/PerformWalRecovery.md)
  - [WaitForWALToBecomeAvailable](../W/WaitForWALToBecomeAvailable.md)

## Notes and Other Information
- This is a static function within xlogrecovery.c, not exposed as a public API
- Uses a 1000ms timeout on the condition variable to periodically check exit conditions
- Provides user-friendly log messages with hints about using pg_wal_replay_resume() to continue
- The pause mechanism is essential for point-in-time recovery scenarios where administrators need to inspect database state before proceeding
- Location: src/backend/access/transam/xlogrecovery.c:2925-2981

## Simplified Source

```c
// Simplified version of recoveryPausesHere
static void recoveryPausesHere(bool endOfRecovery) {
    // Safety checks: only pause when users can connect and promotion isn't triggered
    if (!LocalHotStandbyActive || LocalPromoteIsTriggered)
        return;

    // Log appropriate message based on recovery state
    if (endOfRecovery)
        ereport(LOG, (errmsg("pausing at the end of recovery"),
                     errhint("Execute pg_wal_replay_resume() to promote.")));
    else
        ereport(LOG, (errmsg("recovery has paused"),
                     errhint("Execute pg_wal_replay_resume() to continue.")));

    // Main pause loop: wait until recovery is resumed
    while (GetRecoveryPauseState() != RECOVERY_NOT_PAUSED) {
        // Handle interrupts and check for standby promotion
        HandleStartupProcInterrupts();
        if (CheckForStandbyTrigger())
            return;

        // Confirm we're still in paused state
        ConfirmRecoveryPaused();

        // Sleep with timeout to periodically check conditions
        ConditionVariableTimedSleep(&XLogRecoveryCtl->recoveryNotPausedCV, 1000,
                                   WAIT_EVENT_RECOVERY_PAUSE);
    }

    // Clean up condition variable state
    ConditionVariableCancelSleep();
}
```

Key simplifications made:
- Combined multiple conditional exits into a single early return
- Added descriptive comments for each major logic section
- Preserved the essential pause/resume mechanism and safety checks
- Maintained the condition variable timeout pattern for efficient waiting
- Kept critical error handling and interrupt processing