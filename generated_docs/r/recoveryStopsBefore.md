# recoveryStopsBefore

## Location
[src/backend/access/transam/xlogrecovery.c:2573-2725](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L2573-L2725)

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
  - [xl_xact_parsed_abort](../x/xl_xact_parsed_abort.md): Parsed abort record structure
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

## Simplified Source

```c
// Simplified version of recoveryStopsBefore
static bool recoveryStopsBefore(XLogReaderState *record) {
    bool stopsHere = false;
    uint8 xact_info;
    bool isCommit;
    TimestampTz recordXtime = 0;
    TransactionId recordXid;

    // Only operate during archive recovery, not crash recovery
    if (!ArchiveRecoveryRequested)
        return false;

    // Stop immediately when reaching consistency if requested
    if (recoveryTarget == RECOVERY_TARGET_IMMEDIATE && reachedConsistency) {
        // Set recovery stop information and log
        recoveryStopAfter = false;
        // Clear stop parameters
        return true;
    }

    // Stop before target LSN if specified
    if (recoveryTarget == RECOVERY_TARGET_LSN &&
        !recoveryTargetInclusive &&
        record->ReadRecPtr >= recoveryTargetLSN) {
        // Set LSN stop information and log
        recoveryStopLSN = record->ReadRecPtr;
        return true;
    }

    // Only process transaction commit/abort records beyond this point
    if (XLogRecGetRmid(record) != RM_XACT_ID)
        return false;

    xact_info = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;

    // Parse transaction record to get XID and determine commit/abort
    if (xact_info == XLOG_XACT_COMMIT) {
        isCommit = true;
        recordXid = XLogRecGetXid(record);
    } else if (xact_info == XLOG_XACT_COMMIT_PREPARED) {
        isCommit = true;
        // Parse prepared commit record for XID
        xl_xact_parsed_commit parsed;
        ParseCommitRecord(XLogRecGetInfo(record),
                         (xl_xact_commit *) XLogRecGetData(record),
                         &parsed);
        recordXid = parsed.twophase_xid;
    } else if (xact_info == XLOG_XACT_ABORT) {
        isCommit = false;
        recordXid = XLogRecGetXid(record);
    } else if (xact_info == XLOG_XACT_ABORT_PREPARED) {
        isCommit = false;
        // Parse prepared abort record for XID
        xl_xact_parsed_abort parsed;
        ParseAbortRecord(XLogRecGetInfo(record),
                        (xl_xact_abort *) XLogRecGetData(record),
                        &parsed);
        recordXid = parsed.twophase_xid;
    } else {
        return false;
    }

    // Check XID-based stopping criteria
    if (recoveryTarget == RECOVERY_TARGET_XID && !recoveryTargetInclusive) {
        // Must use exact equality for XID comparison
        stopsHere = (recordXid == recoveryTargetXid);
    }

    // Check time-based stopping criteria
    if (getRecordTimestamp(record, &recordXtime) &&
        recoveryTarget == RECOVERY_TARGET_TIME) {
        // Handle inclusive vs exclusive time comparison
        if (recoveryTargetInclusive)
            stopsHere = (recordXtime > recoveryTargetTime);
        else
            stopsHere = (recordXtime >= recoveryTargetTime);
    }

    // If stopping, set recovery stop information and log the decision
    if (stopsHere) {
        recoveryStopAfter = false;
        recoveryStopXid = recordXid;
        recoveryStopTime = recordXtime;
        recoveryStopLSN = InvalidXLogRecPtr;
        recoveryStopName[0] = '\0';

        // Log recovery stopping decision with transaction details
        if (isCommit) {
            ereport(LOG, (errmsg("recovery stopping before commit of transaction %u, time %s",
                                 recoveryStopXid, timestamptz_to_str(recoveryStopTime))));
        } else {
            ereport(LOG, (errmsg("recovery stopping before abort of transaction %u, time %s",
                                 recoveryStopXid, timestamptz_to_str(recoveryStopTime))));
        }
    }

    return stopsHere;
}
```

Key simplifications made:
- Consolidated similar transaction record parsing branches
- Simplified error handling and memory operations
- Added high-level comments explaining each major section
- Streamlined variable initialization and assignment patterns
- Maintained all essential logic for recovery target evaluation
- Preserved critical XID equality testing and time comparison logic
- Kept logging functionality for debugging recovery decisions