# pgstat_before_server_shutdown

## Location
src/backend/utils/activity/pgstat.c: 462 - 502

## Overview
Handles statistics persistence during server shutdown by flushing pending statistics and writing them to disk. This function ensures statistics data is preserved across normal server shutdowns.

## Definition


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
- : Exit code indicating the type of shutdown (0 for normal, non-zero for irregular)
- : Datum argument (unused but required for callback function signature)

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_report_stat
  - pgstat_write_statsfile
- Called from (representative examples):
  - CheckpointerMain (src/backend/postmaster/checkpointer.c:223)
  - InitPostgres (src/backend/utils/init/postinit.c:809)

## Notes and Other Information
- Must be called by exactly one process during shutdown to avoid conflicts
- Only writes statistics file during normal shutdowns (exit code 0)
- Performs assertions to verify proper statistics system state
- The function is typically registered as a callback during process initialization
- Statistics file writing uses a temporary file and atomic rename for consistency
- Irregular shutdowns skip file writing since the data will be discarded on next startup
- The function is located in src/backend/utils/activity/pgstat.c:462-502