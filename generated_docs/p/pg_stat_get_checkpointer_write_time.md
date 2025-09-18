# pg_stat_get_checkpointer_write_time

## Location
src/backend/utils/adt/pgstatfuncs.c: 1231 - 1238

## Overview
Returns the total time spent by the checkpointer process writing data files to disk, measured in milliseconds since server startup.

## Definition
```c
Datum pg_stat_get_checkpointer_write_time(PG_FUNCTION_ARGS)
```

## Detailed Description
This SQL-callable function provides access to the checkpointer's write time statistics. It retrieves the cumulative time (in milliseconds) that the checkpointer process has spent performing write operations to data files since the PostgreSQL server was started. This timing information is crucial for understanding checkpointer I/O performance and can help identify potential bottlenecks in the storage subsystem. The function converts the internal millisecond value to a double-precision floating-point number for SQL presentation. This metric only includes time spent actually writing to disk, not time spent on other checkpointer activities like syncing or administrative tasks.

## Parameters / Member Variables
- No input parameters (uses PostgreSQL's standard PG_FUNCTION_ARGS macro)

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_fetch_stat_checkpointer](pgstat_fetch_stat_checkpointer.md)
  - PG_RETURN_FLOAT8
- Called from (representative examples):
  - SQL queries via the PostgreSQL function call interface
  - System monitoring and statistics collection queries

## Notes and Other Information
- Returns a double-precision floating-point value representing milliseconds
- Part of PostgreSQL's statistics collection system for monitoring checkpointer performance
- Time measurement requires track_io_timing to be enabled for accurate results
- The underlying data comes from the checkpointer statistics structure maintained by the statistics collector
- Useful for diagnosing I/O performance issues and checkpoint tuning
- Located in src/backend/utils/adt/pgstatfuncs.c:1231-1238