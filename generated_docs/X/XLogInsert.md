# XLogInsert

## Location
src/backend/access/transam/xloginsert.c: 474 - 547

## Overview
XLogInsert is the primary function that finalizes and inserts a constructed WAL record into the Write-Ahead Log, returning the LSN for the inserted record.

## Definition
XLogRecPtr XLogInsert(RmgrId rmid, uint8 info)

## Detailed Description
XLogInsert is the culminating function in PostgreSQL's WAL record construction and insertion process. It takes all the data, buffer references, and flags registered through previous XLogRegister* calls and creates a complete WAL record with the specified resource manager ID (rmid) and info byte.

The function performs several critical operations: validates that XLogBeginInsert() was called, checks info byte validity, handles bootstrap mode specially, determines whether full-page writes are needed, assembles the complete record using XLogRecordAssemble(), and finally inserts it via XLogInsertRecord(). The process may retry if insertion fails due to timing issues with full-page write requirements.

The function implements the fundamental WAL principle "write the log before the data" by returning an LSN that represents the point up to which WAL must be flushed before any associated data pages can be written to disk. This LSN serves as a durability guarantee for the logged operation.

After successful insertion, the function cleans up all registration state via XLogResetInsertion(), preparing for the next WAL record construction cycle.

## Parameters / Member Variables
- rmid: Resource Manager ID identifying which subsystem owns this record type (e.g., RM_HEAP_ID for heap operations, RM_BTREE_ID for B-tree operations)
- info: 8-bit info field containing operation-specific flags and information, with certain bits reserved for system use

## Dependencies
- Functions called/Symbols referenced:
  - [GetFullPageWriteInfo](../G/GetFullPageWriteInfo.md) (determines full-page write requirements)
  - [XLogRecordAssemble](XLogRecordAssemble.md) (assembles the complete WAL record)
  - [XLogInsertRecord](XLogInsertRecord.md) (physically inserts the record into WAL)
  - [XLogResetInsertion](XLogResetInsertion.md) (cleans up insertion state)
  - IsBootstrapProcessingMode (checks for bootstrap mode)
  - RmgrId, XLogRecData, XLogRecPtr (data types)
- Called from (representative examples):
  - [heap_insert](../h/heap_insert.md) (heap tuple insertions)
  - [_bt_insertonpg](../b/_bt_insertonpg.md) (B-tree insertions)
  - [XactLogCommitRecord](XactLogCommitRecord.md) (transaction commits)
  - [CreateCheckPoint](../C/CreateCheckPoint.md) (checkpoint operations)
  - [log_newpage](../l/log_newpage.md) (new page logging)

## Notes and Other Information
- Must be called after XLogBeginInsert() and all XLogRegister* calls
- Returns InvalidXLogRecPtr on failure, requiring retry
- In bootstrap mode, returns a dummy LSN for non-XLOG resource managers
- The returned LSN can be used to set page LSNs for affected data pages
- Handles full-page write decisions dynamically based on current WAL state
- Central function that coordinates the final WAL record insertion process
- Automatically retries insertion if conditions change during assembly