# getRecordTimestamp

## Location
[src/backend/access/transam/xlogrecovery.c:2426-2460](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L2426-L2460)

## Overview
Extracts timestamp information from WAL (Write-Ahead Log) records during PostgreSQL recovery operations.

## Definition

```c
static bool
getRecordTimestamp(XLogReaderState *record, TimestampTz *recordXtime)
```
## Detailed Description
The  function is a utility function used during WAL recovery to extract timestamp information from specific types of WAL records. It examines the record type and extracts the timestamp if available, supporting transaction commit/abort records and restore points. This function is crucial for recovery operations that need to determine when specific database events occurred, particularly for point-in-time recovery scenarios.

The function checks the resource manager ID (rmid) and operation info to determine the record type, then casts the record data to the appropriate structure to extract the timestamp field.

## Parameters / Member Variables
- : Pointer to XLogReaderState containing the WAL record being examined
- : Output parameter - pointer to TimestampTz where the extracted timestamp will be stored

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo: Gets the info field from the WAL record
  - XLogRecGetRmid: Gets the resource manager ID from the WAL record
  - XLogRecGetData: Gets the data portion of the WAL record
- Constants used:
  - XLR_INFO_MASK: Mask for extracting info bits
  - XLOG_XACT_OPMASK: Mask for transaction operation types
  - XLOG_RESTORE_POINT: Restore point record type
  - XLOG_XACT_COMMIT/XLOG_XACT_COMMIT_PREPARED: Transaction commit record types
  - XLOG_XACT_ABORT/XLOG_XACT_ABORT_PREPARED: Transaction abort record types
- Structures used:
  - [xl_restore_point](../x/xl_restore_point.md): Structure for restore point records
  - [xl_xact_commit](../x/xl_xact_commit.md): Structure for transaction commit records
  - [xl_xact_abort](../x/xl_xact_abort.md): Structure for transaction abort records
- Called from:
  - [recoveryStopsBefore](../r/recoveryStopsBefore.md): Uses timestamps for recovery stopping logic
  - [recoveryStopsAfter](../r/recoveryStopsAfter.md): Uses timestamps for recovery stopping logic
  - [recoveryApplyDelay](../r/recoveryApplyDelay.md): Uses timestamps for applying recovery delays

## Notes and Other Information
- Returns  if the record contains a timestamp and it was successfully extracted,  otherwise
- Only specific record types contain timestamps: transaction commit/abort records and restore points
- This is a static function, only accessible within the xlogrecovery.c file
- The function is essential for point-in-time recovery (PITR) functionality
- Supports both regular and prepared transaction commits/aborts