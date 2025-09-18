# pg_stat_get_checkpointer_restartpoints_timed

## Location
[src/backend/utils/adt/pgstatfuncs.c:1195-1200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1195-L1200)

## Overview
Returns the number of restartpoints that were triggered by timeout (scheduled) rather than being explicitly requested.

## Definition
```c
Datum pg_stat_get_checkpointer_restartpoints_timed(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the count of timed restartpoints that have been performed by the checkpointer process. In PostgreSQL standby servers, restartpoints serve a similar purpose to checkpoints on primary servers - they ensure data consistency and provide recovery points. Timed restartpoints are those initiated automatically based on the checkpoint_timeout configuration parameter, as opposed to restartpoints requested due to WAL volume or explicit commands.

## Parameters / Member Variables
- No parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_fetch_stat_checkpointer](pgstat_fetch_stat_checkpointer.md)
  - PG_RETURN_INT64
- Called from (representative examples):
  - SQL queries via pg_stat_get_checkpointer_restartpoints_timed() function
  - Standby server monitoring and statistics collection

## Notes and Other Information
- Returns a 64-bit integer representing the cumulative count since cluster startup
- Specifically tracks restartpoints triggered by timeout rather than WAL volume or explicit requests
- This metric is particularly relevant for standby servers where restartpoints replace checkpoints
- The counter is maintained in the checkpointer statistics structure's restartpoints_timed field
- Useful for tuning checkpoint_timeout and understanding restartpoint patterns on standby servers