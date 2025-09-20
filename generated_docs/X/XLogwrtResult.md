# XLogwrtResult

## Location
[src/backend/access/transam/xlog.c:326-330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L326-L330)

## Overview
XLogwrtResult is a shared-memory data structure that tracks the actual completion status of WAL write and flush operations, recording the byte positions that have already been successfully written to disk and fsynced.

## Definition

```c
typedef struct XLogwrtResult
{
	XLogRecPtr	Write;			/* last byte + 1 written out */
	XLogRecPtr	Flush;			/* last byte + 1 flushed */
} XLogwrtResult;
```
## Detailed Description
XLogwrtResult represents the completion status of Write-Ahead Log (WAL) operations, tracking what has actually been accomplished rather than what is requested. It maintains the byte positions in the WAL that have been successfully written to disk and fsynced, providing the system with knowledge of which records are durably stored.

This structure works in tandem with XLogwrtRqst to implement PostgreSQL's WAL durability mechanism. While XLogwrtRqst indicates what needs to be done, XLogwrtResult tracks what has been completed. The positions are maintained using atomic access in shared memory variables logWriteResult and logFlushResult, with each backend maintaining private copies in LogwrtResult that are updated when convenient.

## Parameters / Member Variables
- `Write`: XLogRecPtr indicating the last byte position + 1 that has been successfully written to disk
- `Flush`: XLogRecPtr indicating the last byte position + 1 that has been successfully fsynced for durability
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtr (data type for WAL record pointers)
- Called from (representative examples):
  - ConvertToXSegs

## Notes and Other Information
- Companion structure to XLogwrtRqst for tracking WAL operation completion
- Maintained using atomic access for thread safety
- Each backend keeps private copies updated when convenient
- Write position advances before Flush position as writing precedes fsyncing
- Critical for determining which WAL records are safely persisted
- Used by recovery and checkpoint processes to understand WAL state