# issue_xlog_fsync

## Location
[src/backend/access/transam/xlog.c:8699-8807](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L8699-L8807)

## Overview
Issues the appropriate type of fsync operation for a WAL file based on the configured synchronization method, with timing instrumentation and comprehensive error handling.

## Definition

```c
void
issue_xlog_fsync(int fd, XLogSegNo segno, TimeLineID tli)
```
## Detailed Description
This function performs file synchronization for WAL (Write-Ahead Log) files using the method specified by the  GUC parameter. It provides multiple synchronization strategies including standard fsync, write-through fsync, and fdatasync, while collecting timing statistics and handling errors with PANIC-level severity.

Key functionality includes:
- Early exit optimization for cases where synchronization is unnecessary (fsync disabled or synchronous write methods)
- Support for multiple platform-specific sync methods (fsync, fsync with write-through, fdatasync)
- I/O timing measurement for performance monitoring when  is enabled
- Wait event reporting for monitoring potentially slow fsync operations
- Comprehensive error handling with detailed error messages and PANIC responses
- Statistics collection for WAL synchronization operations

## Parameters / Member Variables
- : File descriptor for the XLOG file to be synchronized
- : WAL segment number used for error reporting and filename generation
- : Timeline ID used for error reporting and filename generation (must be non-zero)

## Dependencies
- Functions called/Symbols referenced:
  - XLogSegNo
  - [instr_time](instr_time.md)
  - WAL_SYNC_METHOD_OPEN
  - [WAL_SYNC_METHOD_OPEN_DSYNC](../W/WAL_SYNC_METHOD_OPEN_DSYNC.md)
  - INSTR_TIME_SET_CURRENT
  - INSTR_TIME_SET_ZERO
  - pgstat_report_wait_start
  - WAL_SYNC_METHOD_FSYNC
  - pg_fsync_no_writethrough
  - WAL_SYNC_METHOD_FSYNC_WRITETHROUGH
  - [pg_fsync_writethrough](../p/pg_fsync_writethrough.md)
  - WAL_SYNC_METHOD_FDATASYNC
  - pg_fdatasync
  - [XLogFileName](../X/XLogFileName.md)
  - pgstat_report_wait_end
  - INSTR_TIME_ACCUM_DIFF
  - MAXFNAMELEN
  - PANIC
- Called from (representative examples):
  - [XLogWrite](../X/XLogWrite.md)
  - [XLogWalRcvFlush](../X/XLogWalRcvFlush.md)

## Notes and Other Information
- The function optimizes performance by skipping fsync when using O_SYNC or O_DSYNC open flags, as write() calls are already synchronous
- Platform-specific conditional compilation ensures write-through fsync is only used when supported (HAVE_FSYNC_WRITETHROUGH)
- All fsync failures result in PANIC-level errors, indicating these are unrecoverable situations requiring database restart
- I/O timing collection supports performance monitoring and tuning of WAL synchronization operations
- The function maintains WAL sync statistics in PendingWalStats for monitoring and administrative purposes
- Timeline ID assertion ensures valid timeline context for error reporting