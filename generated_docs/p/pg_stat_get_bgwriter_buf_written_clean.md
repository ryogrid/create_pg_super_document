# pg_stat_get_bgwriter_buf_written_clean

## Location
[src/backend/utils/adt/pgstatfuncs.c:1219-1224](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1219-L1224)

## Overview
Returns the number of buffers written by the background writer during its cleaning scan activities since server startup.

## Definition
```c
Datum pg_stat_get_bgwriter_buf_written_clean(PG_FUNCTION_ARGS)
```

## Detailed Description
This SQL-callable function provides access to the background writer's buffer cleaning statistics. It retrieves the cumulative count of buffers that have been written to disk by the background writer process specifically during its cleaning scan operations. The background writer periodically scans the shared buffer pool to write out dirty buffers before they are needed by other processes, helping to reduce the I/O burden during checkpoints and user queries. This statistic specifically tracks buffers written during these proactive cleaning operations, as opposed to buffers written due to buffer replacement pressure.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_fetch_stat_bgwriter](pgstat_fetch_stat_bgwriter.md)
  - PG_RETURN_INT64
- Called from (representative examples):
  - SQL queries via the PostgreSQL function call interface
  - System monitoring and statistics collection queries

## Notes and Other Information
- Returns a 64-bit integer value to accommodate large buffer counts over long server uptimes
- Part of PostgreSQL's statistics collection system for monitoring background writer performance
- Distinguishes between proactive cleaning writes and writes due to buffer replacement pressure
- The underlying data comes from the background writer statistics structure maintained by the statistics collector
- Located in src/backend/utils/adt/pgstatfuncs.c:1219-1224

## Simplified Source

```c
Datum
pg_stat_get_bgwriter_buf_written_clean(PG_FUNCTION_ARGS)
{
    // Return buffers written during background writer cleaning scans
    return pgstat_fetch_stat_bgwriter()->buf_written_clean;
}
```