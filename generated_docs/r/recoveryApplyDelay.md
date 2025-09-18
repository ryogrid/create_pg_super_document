# recoveryApplyDelay

## Location
src/backend/access/transam/xlogrecovery.c: 2982 - 3069

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
  - getRecordTimestamp
  - TimestampTzPlusMilliseconds
  - GetCurrentTimestamp
  - TimestampDifferenceMilliseconds
  - ResetLatch
  - HandleStartupProcInterrupts
  - CheckForStandbyTrigger
  - WaitLatch
- Called from (representative examples):
  - PerformWalRecovery

## Notes and Other Information
- This is a static function within xlogrecovery.c, not exposed as a public API
- Returns true if a delay was actually applied, false otherwise
- The delay mechanism helps reduce conflicts on read replicas by ensuring some temporal separation from the primary
- Only COMMIT records are delayed; ABORT records are not delayed as they don't affect MVCC visibility
- Uses WaitLatch with timeout for efficient waiting and proper signal handling
- The delay can be dynamically recalculated during the wait if recovery_min_apply_delay changes
- Location: src/backend/access/transam/xlogrecovery.c:2982-3069