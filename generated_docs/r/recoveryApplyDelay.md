# recoveryApplyDelay

## Location
[src/backend/access/transam/xlogrecovery.c:2982-3069](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L2982-L3069)

## Overview
Implements a configurable delay mechanism during WAL recovery to ensure certain record types are applied at least recovery_min_apply_delay milliseconds behind the primary server.

## Definition
static bool recoveryApplyDelay(XLogReaderState *record)

## Detailed Description
This function enforces a minimum time delay between when a WAL record was logged on the primary and when it gets applied on the standby. The delay is only applied to COMMIT and COMMIT_PREPARED transaction records, as these are the most critical for maintaining consistency and MVCC behavior.

The function performs several checks before applying delay:
- Only applies delay if recovery_min_apply_delay is configured (> 0)
- Skips delay if the database has not yet reached consistency
- Only applies during archive recovery, not crash recovery
- Only delays COMMIT and COMMIT_PREPARED records

The delay calculation is based on the difference between the WAL record's timestamp and the current time on the standby. During the wait loop, the function periodically checks for interrupts and standby triggers, and recalculates the delay in case the configuration changes.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record being processed, used to extract record type, transaction info, and timestamp

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetRmid
  - XLogRecGetInfo
  - XLOG_XACT_OPMASK
  - XLOG_XACT_COMMIT
  - XLOG_XACT_COMMIT_PREPARED
  - [getRecordTimestamp](../g/getRecordTimestamp.md)
  - TimestampTzPlusMilliseconds
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [TimestampDifferenceMilliseconds](../T/TimestampDifferenceMilliseconds.md)
  - [ResetLatch](../R/ResetLatch.md)
  - [HandleStartupProcInterrupts](../H/HandleStartupProcInterrupts.md)
  - [CheckForStandbyTrigger](../C/CheckForStandbyTrigger.md)
  - [WaitLatch](../W/WaitLatch.md)
- Called from (representative examples):
  - [PerformWalRecovery](../P/PerformWalRecovery.md)

## Notes and Other Information
- This is a static function within xlogrecovery.c, not exposed as a public API
- Returns true if a delay was actually applied, false otherwise
- The delay mechanism helps reduce conflicts on read replicas by ensuring some temporal separation from the primary
- Only COMMIT records are delayed; ABORT records are not delayed as they don't affect MVCC visibility
- Uses WaitLatch with timeout for efficient waiting and proper signal handling
- The delay can be dynamically recalculated during the wait if recovery_min_apply_delay changes
- Location: src/backend/access/transam/xlogrecovery.c:2982-3069

## Simplified Source

```c
// Simplified version of recoveryApplyDelay
static bool recoveryApplyDelay(XLogReaderState *record) {
    uint8 xact_info;
    TimestampTz xtime;
    TimestampTz delayUntil;
    long msecs;

    // Early exits - no delay needed
    if (recovery_min_apply_delay <= 0)
        return false;
    if (!reachedConsistency)
        return false;
    if (!ArchiveRecoveryRequested)
        return false;

    // Only delay COMMIT records, not other transaction types
    if (XLogRecGetRmid(record) != RM_XACT_ID)
        return false;

    xact_info = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;
    if (xact_info != XLOG_XACT_COMMIT && xact_info != XLOG_XACT_COMMIT_PREPARED)
        return false;

    // Get record timestamp and calculate delay target
    if (!getRecordTimestamp(record, &xtime))
        return false;

    delayUntil = TimestampTzPlusMilliseconds(xtime, recovery_min_apply_delay);

    // Check if delay period has already passed
    msecs = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), delayUntil);
    if (msecs <= 0)
        return false;

    // Wait loop with interrupt handling
    while (true) {
        ResetLatch(&XLogRecoveryCtl->recoveryWakeupLatch);

        // Handle interrupts (may change recovery_min_apply_delay)
        HandleStartupProcInterrupts();

        // Check for standby promotion trigger
        if (CheckForStandbyTrigger())
            break;

        // Recalculate delay in case configuration changed
        delayUntil = TimestampTzPlusMilliseconds(xtime, recovery_min_apply_delay);
        msecs = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), delayUntil);

        if (msecs <= 0)
            break;

        // Wait for the calculated delay period
        WaitLatch(&XLogRecoveryCtl->recoveryWakeupLatch,
                  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                  msecs, WAIT_EVENT_RECOVERY_APPLY_DELAY);
    }
    return true;
}
```

Key simplifications made:
- Removed detailed comments explaining time synchronization concerns
- Simplified variable declarations and early exit conditions
- Condensed the main wait loop logic for better readability
- Removed debug logging statement for clarity
- Consolidated similar conditional checks
- Maintained essential algorithm: delay COMMIT records based on configured minimum apply delay