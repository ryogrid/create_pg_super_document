# pg_stat_get_checkpointer_restartpoints_requested

## Location
[src/backend/utils/adt/pgstatfuncs.c:1201-1206](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1201-L1206)

## Overview
Returns the number of restartpoints that were explicitly requested rather than triggered by timeout.

## Definition
```c
Datum pg_stat_get_checkpointer_restartpoints_requested(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the count of requested restartpoints that have been performed by the checkpointer process. On PostgreSQL standby servers, restartpoints fulfill a similar role to checkpoints on primary servers. Requested restartpoints are those initiated due to explicit requests, such as administrative commands or when WAL volume thresholds are reached, as opposed to those triggered automatically by timeout intervals.

## Parameters / Member Variables
- No parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_fetch_stat_checkpointer](pgstat_fetch_stat_checkpointer.md)
  - PG_RETURN_INT64
- Called from (representative examples):
  - SQL queries via pg_stat_get_checkpointer_restartpoints_requested() function
  - Standby server monitoring and statistics collection

## Notes and Other Information
- Returns a 64-bit integer representing the cumulative count since cluster startup
- Tracks restartpoints initiated by explicit requests rather than automatic timeout-based triggers
- This metric complements pg_stat_get_checkpointer_restartpoints_timed for complete restartpoint analysis
- The counter is maintained in the checkpointer statistics structure's restartpoints_requested field
- Particularly relevant for standby servers where restartpoint behavior affects recovery performance
- Useful for understanding workload patterns and tuning restartpoint-related configuration parameters