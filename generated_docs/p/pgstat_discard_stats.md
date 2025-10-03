# pgstat_discard_stats

## Location
[src/backend/utils/activity/pgstat.c:419-461](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L419-L461)

## Overview
Removes the PostgreSQL statistics file from disk and resets all statistics contents in memory. This function is primarily used during WAL recovery after a crash to ensure statistics consistency.

## Definition

```c
void
pgstat_discard_stats(void)
```
## Detailed Description
This function performs a complete cleanup of PostgreSQL statistics data both on disk and in memory. It first attempts to remove the permanent statistics file from the filesystem using the  system call. After file removal, it resets all statistics contents in shared memory to ensure a clean state.

The function is specifically designed to handle crash recovery scenarios where the statistics file may contain inconsistent or corrupted data that doesn't match the current database state after WAL recovery. By discarding the old statistics and starting fresh, the system ensures data integrity.

The function handles file removal errors gracefully:
- If the file doesn't exist (ENOENT), it logs a debug message indicating no action was needed
- For other errors, it reports a log message but continues execution
- On successful removal, it logs a debug message confirming the action

After file operations, the function calls  to reset all statistics counters and timestamps to current values, ensuring the statistics system starts with a clean slate.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - unlink (system call)
  - PGSTAT_STAT_PERMANENT_FILENAME (constant)
  - [pgstat_reset_after_failure](pgstat_reset_after_failure.md)
- Called from (representative examples):
  - [StartupXLOG](../S/StartupXLOG.md) (src/backend/access/transam/xlog.c:5638)

## Notes and Other Information
- This function must be called during server startup, specifically by the startup process or in single-user mode
- The operation is performed even in single-user mode to ensure consistency
- Used primarily during WAL recovery scenarios after crashes
- The function continues execution even if file removal fails, ensuring the statistics reset occurs
- Reset timestamps are set to the current time for fixed-numbered statistics
- All variable-numbered statistics entries are completely dropped
- The function is located in src/backend/utils/activity/pgstat.c:419-461

## Simplified Source

```c
// Simplified version of pgstat_discard_stats
void pgstat_discard_stats(void) {
    int ret;

    // Remove the permanent statistics file from disk
    ret = unlink(PGSTAT_STAT_PERMANENT_FILENAME);

    // Handle file removal results
    if (ret != 0) {
        if (errno == ENOENT) {
            // File didn't exist - log debug message
            elog(DEBUG2, "didn't need to unlink permanent stats file - didn't exist");
        } else {
            // Other error - report but continue
            ereport(LOG, "could not unlink permanent statistics file");
        }
    } else {
        // Success - log debug message
        ereport(DEBUG2, "unlinked permanent statistics file");
    }

    // Reset all statistics contents in memory
    pgstat_reset_after_failure();
}
```

Key simplifications made:
- Removed detailed error message formatting and file path constants for clarity
- Simplified error handling conditions while preserving the logic flow
- Condensed verbose logging statements to essential information
- Added explanatory comments for each major operation
- Preserved the essential two-step process: file removal + memory reset