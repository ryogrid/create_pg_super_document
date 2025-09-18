# WriteZeroPageXlogRec

## Location
src/backend/access/transam/commit_ts.c: 996 - 1006

## Overview
Writes a ZEROPAGE WAL (Write-Ahead Log) record to log the initialization of a new CLOG (commit log) page with all zero values.

## Definition
```c
static void WriteZeroPageXlogRec(int64 pageno)
```

## Detailed Description
This function creates a WAL record to log the zeroing of a CLOG page. When a new CLOG page needs to be created, this function ensures that the page initialization is properly logged for crash recovery and replication purposes. The WAL record contains the page number being zeroed, which allows recovery processes to recreate the same page state during replay. This is essential for maintaining consistency in the commit log across system crashes and for standby servers to maintain synchronized state.

## Parameters / Member Variables
- `pageno`: The CLOG page number that is being zeroed and needs to be logged in the WAL

## Dependencies
- Functions called/Symbols referenced:
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogInsert](../X/XLogInsert.md) (with RM_CLOG_ID, CLOG_ZEROPAGE)
  - CLOG_ZEROPAGE
- Called from (representative examples):
  - [ZeroCLOGPage](../Z/ZeroCLOGPage.md)
  - [ZeroCommitTsPage](../Z/ZeroCommitTsPage.md) (via XactCtl function pointer)

## Notes and Other Information
- Static function, internal to clog.c
- Part of the CLOG (commit log) WAL logging infrastructure
- The WAL record type is CLOG_ZEROPAGE under the RM_CLOG_ID resource manager
- Essential for crash recovery - allows the system to recreate zero pages during WAL replay
- Also used by commit timestamp functionality through the XactCtl function pointer mechanism
- The page number is logged as binary data to minimize WAL record size
- Follows the standard PostgreSQL WAL logging pattern: BeginInsert → RegisterData → Insert