# XlogReadTwoPhaseData

## Location
[src/backend/access/transam/twophase.c:1404-1458](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L1404-L1458)

## Overview
XlogReadTwoPhaseData reads two-phase commit state data directly from the Write-Ahead Log (WAL) at a specified LSN position.

## Definition

```c
static void
XlogReadTwoPhaseData(XLogRecPtr lsn, char **buf, int *len)
```
## Detailed Description
XlogReadTwoPhaseData provides the capability to retrieve two-phase commit state data directly from WAL records, which is used as an alternative to reading from disk files during certain recovery scenarios. It allocates and configures an XLog reader, seeks to the specified LSN, reads and validates the record to ensure it's a PREPARE record, then copies the data payload into a newly allocated buffer. This function is critical for checkpoint operations that move 2PC data from WAL to persistent files and for recovery operations that need to access prepare state before it's been written to disk.

## Parameters / Member Variables
- : XLogRecPtr specifying the WAL position where the two-phase state data is located
- : char** output parameter that receives a pointer to the palloc'd buffer containing the state data
- : int* optional output parameter that receives the length of the data (can be NULL if length is not needed)

## Dependencies
- Functions called/Symbols referenced:
  - [XLogReaderAllocate](XLogReaderAllocate.md)
  - [XLogBeginRead](XLogBeginRead.md)
  - [XLogReadRecord](XLogReadRecord.md)
  - XLogRecGetRmid
  - XLogRecGetInfo
  - XLogRecGetDataLen
  - XLogRecGetData
  - [XLogReaderFree](XLogReaderFree.md)
  - [read_local_xlog_page](../r/read_local_xlog_page.md)
  - [wal_segment_open](../w/wal_segment_open.md)
  - [wal_segment_close](../w/wal_segment_close.md)
- Called from (representative examples):
  - [FinishPreparedTransaction](../F/FinishPreparedTransaction.md)
  - [CheckPointTwoPhase](../C/CheckPointTwoPhase.md)
  - [ProcessTwoPhaseBuffer](../P/ProcessTwoPhaseBuffer.md)
  - [LookupGXact](../L/LookupGXact.md)

## Notes and Other Information
- Static function (internal to twophase.c module)
- Can access WAL during normal operation, similar to WALSender or Logical Decoding
- Used during checkpoint operations to move 2PC data from WAL to twophase files
- Validates that the record at the specified LSN is indeed a XLOG_XACT_PREPARE record
- Allocates memory for the returned buffer using palloc - caller must free
- Provides detailed error messages with LSN information for debugging
- Critical for recovery scenarios where prepare state hasn't been written to disk yet
- Uses XL_ROUTINE macro for setting up WAL reading callbacks
- Part of the WAL-based two-phase commit state management system