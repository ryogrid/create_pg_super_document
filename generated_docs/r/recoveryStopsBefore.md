# recoveryStopsBefore

## Location
src/backend/access/transam/xlogrecovery.c: 2573 - 2725

## Overview
Determines whether point-in-time recovery should stop before applying the current WAL record based on configured recovery targets.

## Definition
```c
static bool recoveryStopsBefore(XLogReaderState *record)
```

## Detailed Description
The `recoveryStopsBefore` function is a core component of PostgreSQL's point-in-time recovery (PITR) system that evaluates whether recovery should halt before processing the current WAL record. It supports multiple recovery target types including immediate consistency, specific LSN positions, transaction IDs, and timestamps. The function examines transaction commit/abort records and compares them against user-specified recovery targets to determine the precise stopping point for recovery operations.

The function implements complex logic to handle different recovery scenarios: stopping immediately upon reaching consistency, stopping before a specific LSN, and stopping before transactions that meet XID or timestamp criteria. It carefully handles both inclusive and exclusive recovery targets and supports prepared transactions through proper parsing of commit/abort records.

## Parameters / Member Variables
- `record`: Pointer to XLogReaderState containing the current WAL record being evaluated for recovery stopping criteria

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetRmid: Gets resource manager ID from WAL record
  - XLogRecGetInfo: Gets info field from WAL record
  - XLogRecGetXid: Extracts transaction ID from WAL record
  - XLogRecGetData: Gets data portion of WAL record
  - [ParseCommitRecord](../P/ParseCommitRecord.md): Parses commit record details for prepared transactions
  - [ParseAbortRecord](../P/ParseAbortRecord.md): Parses abort record details for prepared transactions
  - [getRecordTimestamp](../g/getRecordTimestamp.md): Extracts timestamp from WAL records
  - [timestamptz_to_str](../t/timestamptz_to_str.md): Converts timestamp to string for logging
- Constants used:
  - RECOVERY_TARGET_IMMEDIATE: Recovery target type for immediate stop
  - RECOVERY_TARGET_LSN: Recovery target type for specific LSN
  - RECOVERY_TARGET_XID: Recovery target type for specific transaction ID
  - RECOVERY_TARGET_TIME: Recovery target type for specific timestamp
  - XLOG_XACT_OPMASK: Mask for transaction operation types
  - XLOG_XACT_COMMIT/XLOG_XACT_COMMIT_PREPARED: Commit record types
  - XLOG_XACT_ABORT/XLOG_XACT_ABORT_PREPARED: Abort record types
- Structures used:
  - [xl_xact_commit](../x/xl_xact_commit.md): Structure for commit records
  - [xl_xact_abort](../x/xl_xact_abort.md): Structure for abort records
  - [xl_xact_parsed_commit](../x/xl_xact_parsed_commit.md): Parsed commit record structure
  - xl_xact_parsed_abort: Parsed abort record structure
- Global variables accessed:
  - ArchiveRecoveryRequested: Indicates if archive recovery is active
  - recoveryTarget: Current recovery target type
  - reachedConsistency: Flag indicating consistency has been reached
  - recoveryTargetInclusive: Whether recovery target should be inclusive
  - recoveryTargetLSN/recoveryTargetXid/recoveryTargetTime: Target values
  - recoveryStop* variables: Variables storing recovery stop information
- Called from:
  - [PerformWalRecovery](../P/PerformWalRecovery.md): Main recovery loop that uses this to determine stopping points

## Notes and Other Information
- This is a static function, only accessible within xlogrecovery.c
- Only operates during archive recovery, not crash recovery
- Sets global recovery stop variables when determining to stop recovery
- Handles both regular and prepared (two-phase commit) transactions
- For XID targets, uses exact equality testing due to transaction numbering complexities
- For time targets, handles both inclusive and exclusive stopping logic
- Logs detailed information about recovery stopping decisions
- Critical for implementing PostgreSQL's point-in-time recovery functionality
- Works in conjunction with `recoveryStopsAfter` for complete recovery control