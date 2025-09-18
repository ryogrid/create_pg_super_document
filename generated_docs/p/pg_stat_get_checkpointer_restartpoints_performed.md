# pg_stat_get_checkpointer_restartpoints_performed

## Location
[src/backend/utils/adt/pgstatfuncs.c:1207-1212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1207-L1212)

## Overview
Returns the total number of restartpoints that have been successfully completed by the checkpointer process.

## Definition
```c
Datum pg_stat_get_checkpointer_restartpoints_performed(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the total count of restartpoints that have been successfully performed by the checkpointer process since cluster startup. On PostgreSQL standby servers, restartpoints serve the same fundamental purpose as checkpoints on primary servers - they ensure data consistency and provide recovery points by flushing dirty pages to disk. This metric represents the aggregate of all completed restartpoints regardless of whether they were triggered by timeout, explicit requests, or WAL volume thresholds.

## Parameters / Member Variables
- No parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_fetch_stat_checkpointer](pgstat_fetch_stat_checkpointer.md)
  - PG_RETURN_INT64
- Called from (representative examples):
  - SQL queries via pg_stat_get_checkpointer_restartpoints_performed() function
  - Standby server monitoring and statistics collection

## Notes and Other Information
- Returns a 64-bit integer representing the cumulative count since cluster startup
- Represents the total of all successfully completed restartpoints, combining both timed and requested restartpoints
- This metric is the sum of restartpoints_timed and restartpoints_requested counters
- The counter is maintained in the checkpointer statistics structure's restartpoints_performed field
- Essential for monitoring the overall restartpoint activity on standby servers
- Useful for assessing the frequency and effectiveness of the checkpointer process on standby systems