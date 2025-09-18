# pg_stat_get_checkpointer_sync_time

## Location
src/backend/utils/adt/pgstatfuncs.c: 1239 - 1246

## Overview
Returns the total time spent by the checkpointer process syncing files to disk, measured in milliseconds since server startup.

## Definition
```c
Datum pg_stat_get_checkpointer_sync_time(PG_FUNCTION_ARGS)
```

## Detailed Description
This SQL-callable function provides access to the checkpointer's sync time statistics. It retrieves the cumulative time (in milliseconds) that the checkpointer process has spent performing fsync operations to ensure data files are durably written to permanent storage since the PostgreSQL server was started. Syncing is a critical phase of checkpoint processing that ensures all written data is physically committed to disk before the checkpoint is considered complete. The function converts the internal millisecond value to a double-precision floating-point number for SQL presentation. This metric helps identify potential I/O bottlenecks during the sync phase of checkpoint operations, which can be a significant performance factor especially on storage systems with poor sync performance.

## Parameters / Member Variables
- No input parameters (uses PostgreSQL's standard PG_FUNCTION_ARGS macro)

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_fetch_stat_checkpointer
  - PG_RETURN_FLOAT8
- Called from (representative examples):
  - SQL queries via the PostgreSQL function call interface
  - System monitoring and statistics collection queries

## Notes and Other Information
- Returns a double-precision floating-point value representing milliseconds
- Part of PostgreSQL's statistics collection system for monitoring checkpointer performance
- Time measurement requires track_io_timing to be enabled for accurate results
- The underlying data comes from the checkpointer statistics structure maintained by the statistics collector
- Sync time is typically different from write time and measures the fsync/fdatasync system call duration
- Useful for diagnosing storage system performance and checkpoint tuning
- Located in src/backend/utils/adt/pgstatfuncs.c:1239-1246