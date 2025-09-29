# recoveryStopsAfter

## Location
[src/backend/access/transam/xlogrecovery.c:2726-2885](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L2726-L2885)

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
  - [getRecordTimestamp](../g/getRecordTimestamp.md): Extracts timestamp from WAL records
  - [SetLatestXTime](../S/SetLatestXTime.md): Updates the latest transaction time tracker
  - [ParseCommitRecord](../P/ParseCommitRecord.md): Parses commit record details for prepared transactions
  - [ParseAbortRecord](../P/ParseAbortRecord.md): Parses abort record details for prepared transactions
  - [strlcpy](../s/strlcpy.md): Safe string copy function
  - [timestamptz_to_str](../t/timestamptz_to_str.md): Converts timestamp to string for logging
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
  - [xl_restore_point](../x/xl_restore_point.md): Structure for restore point records
  - [xl_xact_commit](../x/xl_xact_commit.md): Structure for commit records
  - [xl_xact_abort](../x/xl_xact_abort.md): Structure for abort records
  - [xl_xact_parsed_commit](../x/xl_xact_parsed_commit.md): Parsed commit record structure
  - [xl_xact_parsed_abort](../x/xl_xact_parsed_abort.md): Parsed abort record structure
- Global variables accessed:
  - ArchiveRecoveryRequested: Indicates if archive recovery is active
  - recoveryTarget: Current recovery target type
  - recoveryTargetInclusive: Whether recovery target should be inclusive
  - recoveryTargetName/recoveryTargetLSN/recoveryTargetXid: Target values
  - reachedConsistency: Flag indicating consistency has been reached
  - recoveryStop* variables: Variables storing recovery stop information
- Called from:
  - [PerformWalRecovery](../P/PerformWalRecovery.md): Main recovery loop that uses this after applying records

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

## Simplified Source

```c
// Simplified version of recoveryStopsAfter
static bool recoveryStopsAfter(XLogReaderState *record) {
    uint8 info, xact_info, rmid;
    TimestampTz recordXtime = 0;

    // Skip if not in archive recovery (crash recovery mode)
    if (!ArchiveRecoveryRequested)
        return false;

    info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    rmid = XLogRecGetRmid(record);

    // Check for named restore point target
    if (recoveryTarget == RECOVERY_TARGET_NAME &&
        rmid == RM_XLOG_ID && info == XLOG_RESTORE_POINT) {

        xl_restore_point *restoreData = (xl_restore_point *) XLogRecGetData(record);

        if (strcmp(restoreData->rp_name, recoveryTargetName) == 0) {
            // Found target restore point - stop recovery
            setRecoveryStopInfo(InvalidTransactionId, InvalidXLogRecPtr,
                              record, restoreData->rp_name);
            ereport(LOG, (errmsg("recovery stopping at restore point \"%s\"",
                                recoveryStopName)));
            return true;
        }
    }

    // Check for inclusive LSN target
    if (recoveryTarget == RECOVERY_TARGET_LSN &&
        recoveryTargetInclusive &&
        record->ReadRecPtr >= recoveryTargetLSN) {

        setRecoveryStopInfo(InvalidTransactionId, record->ReadRecPtr, record, NULL);
        ereport(LOG, (errmsg("recovery stopping after WAL location \"%X/%X\"",
                            LSN_FORMAT_ARGS(recoveryStopLSN))));
        return true;
    }

    // Only process transaction records from here
    if (rmid != RM_XACT_ID)
        return false;

    xact_info = info & XLOG_XACT_OPMASK;

    // Handle commit/abort transactions
    if (isTransactionEndRecord(xact_info)) {
        // Update latest transaction timestamp
        if (getRecordTimestamp(record, &recordXtime))
            SetLatestXTime(recordXtime);

        // Extract transaction ID based on record type
        TransactionId recordXid = extractTransactionId(record, xact_info);

        // Check for inclusive XID target
        if (recoveryTarget == RECOVERY_TARGET_XID &&
            recoveryTargetInclusive &&
            recordXid == recoveryTargetXid) {

            setRecoveryStopInfo(recordXid, InvalidXLogRecPtr, record, NULL);
            recoveryStopTime = recordXtime;

            logTransactionStop(xact_info, recordXid);
            return true;
        }
    }

    // Check for immediate stop after consistency
    if (recoveryTarget == RECOVERY_TARGET_IMMEDIATE && reachedConsistency) {
        ereport(LOG, (errmsg("recovery stopping after reaching consistency")));
        setRecoveryStopInfo(InvalidTransactionId, InvalidXLogRecPtr, record, NULL);
        return true;
    }

    return false;
}
```

Key simplifications made:
- Consolidated recovery stop information setting into helper function concept `setRecoveryStopInfo()`
- Extracted transaction ID parsing logic into helper concept `extractTransactionId()`
- Simplified transaction end detection with helper concept `isTransactionEndRecord()`
- Abstracted detailed commit/abort record parsing for clarity
- Removed verbose error handling and detailed logging for core logic focus
- Consolidated similar recovery target checks into clearer conditional blocks
- Simplified variable assignments and reduced repetitive code patterns