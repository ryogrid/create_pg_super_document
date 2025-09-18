# replorigin_redo

## Location
[src/backend/replication/logical/origin.c:827-887](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L827-L887)

## Overview
Processes replication origin-related WAL records during recovery, handling both SET and DROP operations for replication origins.

## Definition


## Detailed Description
replorigin_redo is a WAL record replay function that processes replication origin-related operations during PostgreSQL recovery. It handles two types of operations: XLOG_REPLORIGIN_SET (which advances the replication progress of a specific origin) and XLOG_REPLORIGIN_DROP (which removes/resets a replication origin state). The function extracts the operation type from the WAL record info and executes the corresponding action, ensuring that replication state is properly maintained during recovery scenarios.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record to be processed

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo
  - XLogRecGetData
  - [replorigin_advance](replorigin_advance.md)
  - elog (PANIC level)
  - [xl_replorigin_set](../x/xl_replorigin_set.md) (struct)
  - [xl_replorigin_drop](../x/xl_replorigin_drop.md) (struct)
  - ReplicationState (struct)
  - XLOG_REPLORIGIN_SET
  - XLOG_REPLORIGIN_DROP
  - XLR_INFO_MASK
  - InvalidRepOriginId
  - InvalidXLogRecPtr
- Called from (representative examples):
  - WAL recovery system (via function pointer in rmgr table)

## Notes and Other Information
- This function is part of the resource manager interface for replication origins
- XLOG_REPLORIGIN_SET operations call replorigin_advance with the backward and WAL log flags appropriately set
- XLOG_REPLORIGIN_DROP operations iterate through all replication slots to find and reset the matching origin
- Uses PANIC level error reporting for unknown operation codes, indicating critical system consistency issues
- Essential for maintaining replication state consistency during crash recovery and standby replay