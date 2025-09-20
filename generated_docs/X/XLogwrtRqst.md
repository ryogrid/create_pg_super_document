# XLogwrtRqst

## Location
[src/backend/access/transam/xlog.c:320-324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L320-L324)

## Overview
XLogwrtRqst is a shared-memory data structure that tracks byte positions in the Write-Ahead Log (WAL) that need to be written to disk and/or fsynced to ensure durability.

## Definition

```c
typedef struct XLogwrtRqst
{
	XLogRecPtr	Write;			/* last byte + 1 to write out */
	XLogRecPtr	Flush;			/* last byte + 1 to flush */
} XLogwrtRqst;
```
## Detailed Description
XLogwrtRqst represents write and flush requests for the WAL system. It indicates byte positions that PostgreSQL needs to write and/or fsync the log up to, ensuring that all records before those points are properly persisted to disk. This structure is part of PostgreSQL's WAL control mechanism that manages the durability guarantees of the database.

The structure is used in conjunction with XLogwrtResult to track both what needs to be done (requests) and what has already been completed (results). The request bookkeeping uses a shared XLogCtl->LogwrtRqst variable protected by info_lck spinlock.

## Parameters / Member Variables
- `Write`: XLogRecPtr indicating the last byte position + 1 that needs to be written to disk
- `Flush`: XLogRecPtr indicating the last byte position + 1 that needs to be fsynced for durability
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtr (data type for WAL record pointers)
- Called from (representative examples):
  - [XLogCtlData](XLogCtlData.md) (contains LogwrtRqst member)
  - RefreshXLogWriteResult
  - [AdvanceXLInsertBuffer](../A/AdvanceXLInsertBuffer.md)
  - [XLogWrite](XLogWrite.md)
  - [XLogFlush](XLogFlush.md)
  - [XLogBackgroundFlush](XLogBackgroundFlush.md)

## Notes and Other Information
- Part of PostgreSQL's shared-memory WAL control structures
- Protected by info_lck spinlock for atomic access
- Write position typically advances before Flush position
- Critical for ensuring ACID durability properties
- Used in coordination with WAL buffer management and background writer processes