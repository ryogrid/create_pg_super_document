# pg_stat_get_checkpointer_buffers_written

## Location
src/backend/utils/adt/pgstatfuncs.c: 1213 - 1218

## Overview
Returns the total number of buffers written to disk by the checkpointer process since server startup.

## Definition
```c
Datum pg_stat_get_checkpointer_buffers_written(PG_FUNCTION_ARGS)
```

## Detailed Description
This SQL-callable function provides access to the checkpointer's buffer write statistics. It retrieves the cumulative count of disk buffers that have been written by the checkpointer process since the PostgreSQL server was started. The checkpointer is responsible for periodically writing dirty buffers from the shared buffer pool to permanent storage, and this statistic tracks how many such write operations have occurred. This metric is useful for monitoring checkpointer activity and I/O patterns.

## Parameters / Member Variables
- No input parameters (uses PostgreSQL's standard PG_FUNCTION_ARGS macro)

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_fetch_stat_checkpointer
  - PG_RETURN_INT64
- Called from (representative examples):
  - SQL queries via the PostgreSQL function call interface
  - System monitoring and statistics collection queries

## Notes and Other Information
- Returns a 64-bit integer value to accommodate large buffer counts over long server uptimes
- Part of PostgreSQL's statistics collection system for monitoring checkpointer performance
- The underlying data comes from the checkpointer statistics structure maintained by the statistics collector
- Located in src/backend/utils/adt/pgstatfuncs.c:1213-1218