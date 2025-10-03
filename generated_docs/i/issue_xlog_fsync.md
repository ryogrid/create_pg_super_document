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
- `fd`: File descriptor for the XLOG file to be synchronized
- `segno`: WAL segment number used for error reporting and filename generation
- `tli`: Timeline ID used for error reporting and filename generation (must be non-zero)
## Dependencies
- Functions called/Symbols referenced:
  - XLogSegNo
  - [instr_time](instr_time.md)
  - WAL_SYNC_METHOD_OPEN
  - [WAL_SYNC_METHOD_OPEN_DSYNC](../W/WAL_SYNC_METHOD_OPEN_DSYNC.md)
  - INSTR_TIME_SET_CURRENT
  - INSTR_TIME_SET_ZERO
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)
  - WAL_SYNC_METHOD_FSYNC
  - [pg_fsync_no_writethrough](../p/pg_fsync_no_writethrough.md)
  - WAL_SYNC_METHOD_FSYNC_WRITETHROUGH
  - [pg_fsync_writethrough](../p/pg_fsync_writethrough.md)
  - WAL_SYNC_METHOD_FDATASYNC
  - [pg_fdatasync](../p/pg_fdatasync.md)
  - [XLogFileName](../X/XLogFileName.md)
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md)
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

## Simplified Source

```c
// Simplified version of issue_xlog_fsync
void issue_xlog_fsync(int fd, XLogSegNo segno, TimeLineID tli) {
    char *error_msg = NULL;
    instr_time timing_start;

    // Early exit: Skip fsync if disabled or using synchronous write methods
    if (!enableFsync ||
        wal_sync_method == WAL_SYNC_METHOD_OPEN ||
        wal_sync_method == WAL_SYNC_METHOD_OPEN_DSYNC) {
        return;
    }

    // Start timing measurement if enabled
    if (track_wal_io_timing) {
        INSTR_TIME_SET_CURRENT(timing_start);
    }

    // Begin wait event reporting for monitoring
    pgstat_report_wait_start(WAIT_EVENT_WAL_SYNC);

    // Execute appropriate fsync method based on configuration
    switch (wal_sync_method) {
        case WAL_SYNC_METHOD_FSYNC:
            if (pg_fsync_no_writethrough(fd) != 0) {
                error_msg = _("could not fsync file \"%s\": %m");
            }
            break;

        case WAL_SYNC_METHOD_FSYNC_WRITETHROUGH:
            if (pg_fsync_writethrough(fd) != 0) {
                error_msg = _("could not fsync write-through file \"%s\": %m");
            }
            break;

        case WAL_SYNC_METHOD_FDATASYNC:
            if (pg_fdatasync(fd) != 0) {
                error_msg = _("could not fdatasync file \"%s\": %m");
            }
            break;

        default:
            // Invalid sync method - this is a configuration error
            ereport(PANIC, ...);
            break;
    }

    // Handle fsync failure with PANIC (unrecoverable error)
    if (error_msg) {
        char filename[MAXFNAMELEN];
        XLogFileName(filename, tli, segno, wal_segment_size);
        ereport(PANIC, (errmsg(error_msg, filename)));
    }

    // End wait event reporting
    pgstat_report_wait_end();

    // Record timing statistics if enabled
    if (track_wal_io_timing) {
        instr_time timing_end;
        INSTR_TIME_SET_CURRENT(timing_end);
        INSTR_TIME_ACCUM_DIFF(PendingWalStats.wal_sync_time, timing_end, timing_start);
    }

    // Increment sync operation counter
    PendingWalStats.wal_sync++;
}
```

Key simplifications made:
- Removed detailed error handling code for clarity while preserving error semantics
- Consolidated platform-specific conditional compilation into single case
- Abstracted complex timing macros with descriptive comments
- Simplified variable names (start → timing_start, msg → error_msg)
- Focused on the main execution flow: check config → time operation → sync → handle errors → record stats
- Removed unreachable case assertions and detailed error construction code