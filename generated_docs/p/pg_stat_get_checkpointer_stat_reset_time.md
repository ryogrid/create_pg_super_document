# pg_stat_get_checkpointer_stat_reset_time

## Location
[src/backend/utils/adt/pgstatfuncs.c:1247-1252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1247-L1252)

## Overview
This function returns the timestamp of when checkpointer statistics were last reset in PostgreSQL.

## Definition
```c
Datum pg_stat_get_checkpointer_stat_reset_time(PG_FUNCTION_ARGS)
```

## Detailed Description
This is a SQL-callable function that provides access to the checkpointer statistics reset timestamp. It retrieves the timestamp indicating when the checkpointer process statistics were last reset or when the system was started. The function is a simple wrapper that calls pgstat_fetch_stat_checkpointer() to get the checkpointer statistics structure and returns its stat_reset_timestamp field.

## Parameters / Member Variables
This function takes no parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function calling convention).

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_fetch_stat_checkpointer](pgstat_fetch_stat_checkpointer.md)
  - PG_RETURN_TIMESTAMPTZ
- Called from (representative examples):
  - This function is typically called from SQL queries accessing pg_stat_checkpointer system view

## Notes and Other Information
- Returns a TIMESTAMPTZ (timestamp with timezone) value
- This is part of PostgreSQL's statistics collection system for monitoring checkpointer performance
- The timestamp helps administrators understand when statistics were last reset for proper interpretation of cumulative counters

## Simplified Source

```c
Datum
pg_stat_get_checkpointer_stat_reset_time(PG_FUNCTION_ARGS)
{
    // Return timestamp when checkpointer statistics were last reset
    return pgstat_fetch_stat_checkpointer()->stat_reset_timestamp;
}
```