# pg_stat_get_checkpointer_num_requested

## Location
src/backend/utils/adt/pgstatfuncs.c: 1189 - 1194

## Overview
Returns the total number of checkpoint requests made to the checkpointer process since database cluster startup.

## Definition
```c
Datum pg_stat_get_checkpointer_num_requested(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the count of checkpoint requests that have been made to the checkpointer background process. This includes both automatic checkpoints triggered by PostgreSQL's checkpoint scheduling and manual checkpoint requests initiated by administrative commands like CHECKPOINT. The counter accumulates since the last database cluster startup and provides insight into checkpoint activity levels.

## Parameters / Member Variables
- No parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_fetch_stat_checkpointer
  - PG_RETURN_INT64
- Called from (representative examples):
  - SQL queries via pg_stat_get_checkpointer_num_requested() function
  - System monitoring and statistics collection

## Notes and Other Information
- Returns a 64-bit integer representing the cumulative count
- The counter is maintained in the checkpointer statistics structure's num_requested field
- This metric is useful for monitoring checkpoint frequency and database write activity
- Related to other checkpointer statistics functions for comprehensive checkpoint monitoring