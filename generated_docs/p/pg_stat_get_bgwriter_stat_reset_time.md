# pg_stat_get_bgwriter_stat_reset_time

## Location
src/backend/utils/adt/pgstatfuncs.c: 1253 - 1258

## Overview
This function returns the timestamp of when background writer statistics were last reset in PostgreSQL.

## Definition
```c
Datum pg_stat_get_bgwriter_stat_reset_time(PG_FUNCTION_ARGS)
```

## Detailed Description
This is a SQL-callable function that provides access to the background writer statistics reset timestamp. It retrieves the timestamp indicating when the background writer process statistics were last reset or when the system was started. The function is a simple wrapper that calls pgstat_fetch_stat_bgwriter() to get the background writer statistics structure and returns its stat_reset_timestamp field.

## Parameters / Member Variables
This function takes no parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function calling convention).

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_fetch_stat_bgwriter
  - PG_RETURN_TIMESTAMPTZ
- Called from (representative examples):
  - This function is typically called from SQL queries accessing pg_stat_bgwriter system view

## Notes and Other Information
- Returns a TIMESTAMPTZ (timestamp with timezone) value
- This is part of PostgreSQL's statistics collection system for monitoring background writer performance
- The timestamp helps administrators understand when statistics were last reset for proper interpretation of cumulative counters
- Works similarly to pg_stat_get_checkpointer_stat_reset_time but for background writer statistics