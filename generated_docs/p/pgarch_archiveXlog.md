# pgarch_archiveXlog

## Location
[src/backend/postmaster/pgarch.c:516-642](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/pgarch.c#L516-L642)

## Overview
Archives a single WAL file by invoking the configured archive callback with comprehensive error handling and resource cleanup.

## Definition
static bool pgarch_archiveXlog(char *xlog)

## Detailed Description
pgarch_archiveXlog handles the archival of a single WAL file specified by the xlog parameter. The function serves as a critical wrapper around the actual archive callback, providing robust error handling and resource management:

1. **Process Status Display**: Updates the process title to show current archival activity
2. **Memory Context Management**: Switches to archive_context for safe memory operations
3. **Exception Handling**: Implements a custom sigsetjmp/siglongjmp error handler to convert ERRORs into return values rather than process restarts
4. **Archive Callback Invocation**: Calls the configured archive module's archive_file_cb function
5. **Resource Cleanup**: Performs comprehensive cleanup of various PostgreSQL subsystems on error
6. **Activity Reporting**: Updates process status to reflect success or failure

The function's sophisticated error handling prevents the archiver process from restarting on most errors, instead allowing retry logic in the calling function. It cleans up locks, timeouts, condition variables, statistics, auxiliary processes, file handles, and hash tables when errors occur.

## Parameters / Member Variables
- : Character pointer to the WAL filename to be archived

## Dependencies
- Functions called/Symbols referenced:
  - [set_ps_display](../s/set_ps_display.md) (update process title display)
  - sigsetjmp (set up exception handling)
  - [EmitErrorReport](../E/EmitErrorReport.md) (log errors)
  - HOLD_INTERRUPTS/RESUME_INTERRUPTS (interrupt control)
  - [disable_all_timeouts](../d/disable_all_timeouts.md) (cleanup timeouts)
  - [LWLockReleaseAll](../L/LWLockReleaseAll.md) (release lightweight locks)
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md) (cancel condition variable waits)
  - [pgstat_report_wait_end](pgstat_report_wait_end.md) (end statistics wait reporting)
  - [ReleaseAuxProcessResources](../R/ReleaseAuxProcessResources.md) (release auxiliary process resources)
  - [AtEOXact_Files](../A/AtEOXact_Files.md)/AtEOXact_HashTables (end-of-transaction cleanup)
  - [MemoryContextReset](../M/MemoryContextReset.md) (reset memory context)
  - [FlushErrorState](../F/FlushErrorState.md) (clear error state)
- Constants used:
  - MAXFNAMELEN, XLOGDIR
- Called from (representative examples):
  - [pgarch_ArchiverCopyLoop](pgarch_ArchiverCopyLoop.md) (main archival loop)

## Notes and Other Information
- Returns true on successful archival, false on failure
- This is a static function internal to the pgarch.c module
- Implements comprehensive error recovery to prevent archiver process restarts
- Uses callback-based architecture through ArchiveCallbacks->archive_file_cb
- The error handling is designed to be more granular than the default PostgreSQL ERROR handling
- Memory context switching ensures clean resource management during archival operations
- Process status updates provide visibility into archiver activity for system monitoring

## Simplified Source

```c
static bool pgarch_archiveXlog(char *xlog) {
    sigjmp_buf local_sigjmp_buf;
    MemoryContext oldcontext;
    char pathname[MAXPGPATH];
    char activitymsg[MAXFNAMELEN + 16];
    bool ret;

    // Build full pathname and update process status
    snprintf(pathname, MAXPGPATH, XLOGDIR "/%s", xlog);
    snprintf(activitymsg, sizeof(activitymsg), "archiving %s", xlog);
    set_ps_display(activitymsg);

    // Switch to archive memory context
    oldcontext = MemoryContextSwitchTo(archive_context);

    // Set up error handling to catch failures without restarting archiver
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        // Error occurred - cleanup and return failure
        error_context_stack = NULL;
        HOLD_INTERRUPTS();

        // Log the error
        EmitErrorReport();

        // Comprehensive cleanup of PostgreSQL subsystems
        disable_all_timeouts(false);
        LWLockReleaseAll();
        ConditionVariableCancelSleep();
        pgstat_report_wait_end();
        ReleaseAuxProcessResources(false);
        AtEOXact_Files(false);
        AtEOXact_HashTables(false);

        // Restore memory context and clear error state
        MemoryContextSwitchTo(oldcontext);
        FlushErrorState();
        MemoryContextReset(archive_context);

        PG_exception_stack = NULL;
        RESUME_INTERRUPTS();

        ret = false;
    } else {
        // Normal path - attempt archival
        PG_exception_stack = &local_sigjmp_buf;

        // Call the archive module to do the actual work
        ret = ArchiveCallbacks->archive_file_cb(archive_module_state, xlog, pathname);

        // Cleanup
        PG_exception_stack = NULL;
        MemoryContextSwitchTo(oldcontext);
        MemoryContextReset(archive_context);
    }

    // Update process status with result
    if (ret)
        snprintf(activitymsg, sizeof(activitymsg), "last was %s", xlog);
    else
        snprintf(activitymsg, sizeof(activitymsg), "failed on %s", xlog);
    set_ps_display(activitymsg);

    return ret;
}
```