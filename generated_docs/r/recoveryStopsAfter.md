# recoveryStopsAfter

## Location
src/backend/access/transam/xlogrecovery.c: 2726 - 2885

## Overview
Determines whether point-in-time recovery should stop after applying the current WAL record, complementing recoveryStopsBefore for complete recovery control.

## Definition
```c
static bool recoveryStopsAfter(XLogReaderState *record)
```

## Detailed Description
The `recoveryStopsAfter` function is the complementary counterpart to `recoveryStopsBefore` in PostgreSQL's point-in-time recovery system. It evaluates whether recovery should halt after processing and applying the current WAL record. This function handles recovery scenarios that require inclusive stopping behavior, such as recovery to named restore points, inclusive LSN targets, and inclusive transaction ID targets.

A key additional responsibility of this function is tracking the timestamp of the latest applied commit/abort transaction, updating the system's knowledge of the most recent transaction completion time during recovery. This function supports multiple recovery target types and handles both regular and prepared (two-phase commit) transactions with proper parsing and extraction of transaction details.

## Parameters / Member Variables
- `record`: Pointer to XLogReaderState containing the current WAL record that has just been applied

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo: Gets info field from WAL record
  - XLogRecGetRmid: Gets resource manager ID from WAL record
  - XLogRecGetData: Gets data portion of WAL record
  - XLogRecGetXid: Extracts transaction ID from WAL record
  - getRecordTimestamp: Extracts timestamp from WAL records
  - SetLatestXTime: Updates the latest transaction time tracker
  - ParseCommitRecord: Parses commit record details for prepared transactions
  - ParseAbortRecord: Parses abort record details for prepared transactions
  - strlcpy: Safe string copy function
  - timestamptz_to_str: Converts timestamp to string for logging
- Constants used:
  - XLR_INFO_MASK: Mask for info field bits
  - RECOVERY_TARGET_NAME: Recovery target type for named restore points
  - RECOVERY_TARGET_LSN: Recovery target type for specific LSN
  - RECOVERY_TARGET_XID: Recovery target type for specific transaction ID
  - RECOVERY_TARGET_IMMEDIATE: Recovery target type for immediate stop
  - XLOG_RESTORE_POINT: Restore point record type
  - XLOG_XACT_OPMASK: Mask for transaction operation types
  - XLOG_XACT_COMMIT/XLOG_XACT_COMMIT_PREPARED: Commit record types
  - XLOG_XACT_ABORT/XLOG_XACT_ABORT_PREPARED: Abort record types
  - MAXFNAMELEN: Maximum filename length constant
- Structures used:
  - xl_restore_point: Structure for restore point records
  - xl_xact_commit: Structure for commit records
  - xl_xact_abort: Structure for abort records
  - xl_xact_parsed_commit: Parsed commit record structure
  - xl_xact_parsed_abort: Parsed abort record structure
- Global variables accessed:
  - ArchiveRecoveryRequested: Indicates if archive recovery is active
  - recoveryTarget: Current recovery target type
  - recoveryTargetInclusive: Whether recovery target should be inclusive
  - recoveryTargetName/recoveryTargetLSN/recoveryTargetXid: Target values
  - reachedConsistency: Flag indicating consistency has been reached
  - recoveryStop* variables: Variables storing recovery stop information
- Called from:
  - PerformWalRecovery: Main recovery loop that uses this after applying records

## Notes and Other Information
- This is a static function, only accessible within xlogrecovery.c
- Only operates during archive recovery, not crash recovery
- Works in conjunction with `recoveryStopsBefore` for complete recovery control
- Updates global recovery stop variables when determining to stop recovery
- Maintains the latest transaction timestamp through SetLatestXTime calls
- Handles named restore points by comparing restore point names
- For XID targets, uses exact equality testing like recoveryStopsBefore
- Supports both inclusive LSN recovery and named restore point recovery
- Logs detailed information about recovery stopping decisions
- Essential for implementing PostgreSQL's comprehensive point-in-time recovery system
- Processes both regular and prepared transactions with proper parsing