# pg_stat_force_next_flush

## Location
[src/backend/utils/adt/pgstatfuncs.c:1678-1687](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1678-L1687)

## Overview
A PostgreSQL function that forces the statistics system to flush pending statistics data to the statistics collector at the next available opportunity.

## Definition

```c
Datum
pg_stat_force_next_flush(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides a SQL-callable interface to force the flushing of accumulated statistics data to the statistics collector. PostgreSQL normally batches statistics updates and flushes them periodically to reduce overhead, but this function allows administrators to force an immediate flush on the next statistics reporting cycle. This ensures that any pending statistics changes from the current backend are sent to the statistics collector promptly, making them available for queries against the pg_stat_* system views.

The function is useful in scenarios where immediate visibility of statistics updates is required, such as after bulk operations or when monitoring systems need fresh data without waiting for the normal flush interval.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure (no actual parameters used)
## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_force_next_flush](pgstat_force_next_flush.md) (forces the internal statistics flush)
  - PG_RETURN_VOID (PostgreSQL macro to return void from a function)
- Called from (representative examples):
  - SQL queries using pg_stat_force_next_flush() function

## Notes and Other Information
- This function returns void as it performs a side effect rather than returning data
- The flush occurs at the next opportunity, not immediately when the function is called
- Useful for monitoring applications that need timely statistics updates
- Only affects the current backend's pending statistics, not system-wide statistics flushing
- No special privileges are required to call this function beyond normal database access
- Commonly used in conjunction with pg_stat_clear_snapshot() to get fresh statistics data

## Simplified Source

```c
Datum
pg_stat_force_next_flush(PG_FUNCTION_ARGS)
{
    // Force statistics to be reported at the next occasion
    pgstat_force_next_flush();

    PG_RETURN_VOID();
}
```