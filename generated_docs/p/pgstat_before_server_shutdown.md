# pgstat_before_server_shutdown

## Location
[src/backend/utils/activity/pgstat.c:462-502](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L462-L502)

## Overview
Handles statistics persistence during server shutdown by flushing pending statistics and writing them to disk. This function ensures statistics data is preserved across normal server shutdowns.

## Definition

```c
void
pgstat_before_server_shutdown(int code, Datum arg)
```
## Detailed Description
This function is responsible for preserving PostgreSQL statistics data during server shutdown. It must be called by exactly one process during regular server shutdowns to prevent statistics loss. The function performs a two-phase operation: first flushing any pending statistics updates from the current process, then writing all statistics to the permanent statistics file.

The function includes several safety checks to ensure it's called in the correct context:
- Verifies that shared memory is available and not already marked as shutdown
- Confirms that the statistics system is properly initialized and not yet shut down

The actual file writing only occurs during normal shutdowns (exit code 0). For irregular shutdowns, the function skips writing statistics because the shutdown sequence isn't coordinated to ensure this backend shuts down last, and  would be called during the next startup anyway.

When writing statistics during normal shutdown, the function:
1. Flushes pending statistics updates using 
2. Marks the shared memory as shutdown by setting 
3. Writes all statistics to the permanent file via 

## Parameters / Member Variables
- `code`: Exit code indicating the type of shutdown (0 for normal, non-zero for irregular)
- `arg`: Datum argument (unused but required for callback function signature)
## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_report_stat](pgstat_report_stat.md)
  - [pgstat_write_statsfile](pgstat_write_statsfile.md)
- Called from (representative examples):
  - [CheckpointerMain](../C/CheckpointerMain.md) (src/backend/postmaster/checkpointer.c:223)
  - [InitPostgres](../I/InitPostgres.md) (src/backend/utils/init/postinit.c:809)

## Notes and Other Information
- Must be called by exactly one process during shutdown to avoid conflicts
- Only writes statistics file during normal shutdowns (exit code 0)
- Performs assertions to verify proper statistics system state
- The function is typically registered as a callback during process initialization
- Statistics file writing uses a temporary file and atomic rename for consistency
- Irregular shutdowns skip file writing since the data will be discarded on next startup
- The function is located in src/backend/utils/activity/pgstat.c:462-502

## Simplified Source

```c
// Simplified version of pgstat_before_server_shutdown
void pgstat_before_server_shutdown(int code, Datum arg) {
    // Verify statistics system is properly initialized
    Assert(pgStatLocal.shmem != NULL);
    Assert(!pgStatLocal.shmem->is_shutdown);
    Assert(pgstat_is_initialized && !pgstat_is_shutdown);

    // Flush any pending statistics from this process
    pgstat_report_stat(true);

    // Only write statistics file during normal shutdown (code == 0)
    // Skip during irregular shutdowns to avoid coordination issues
    if (code == 0) {
        // Mark shared memory as shutdown
        pgStatLocal.shmem->is_shutdown = true;

        // Write all statistics to persistent file
        pgstat_write_statsfile();
    }
}
```

Key simplifications made:
- Condensed multi-line comments into single-line explanations
- Grouped related assertions together with unified comment
- Simplified the shutdown condition explanation
- Maintained the essential two-phase logic: flush then write
- Preserved all critical functionality and error checking