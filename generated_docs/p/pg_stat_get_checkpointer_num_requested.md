# pg_stat_get_checkpointer_num_requested

## Location
[src/backend/utils/adt/pgstatfuncs.c:1189-1194](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1189-L1194)

## Overview
Returns the total number of checkpoint requests made to the checkpointer process since database cluster startup.

## Definition
```c
Datum pg_stat_get_checkpointer_num_requested(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the count of checkpoint requests that have been made to the checkpointer background process. This includes both automatic checkpoints triggered by PostgreSQL's checkpoint scheduling and manual checkpoint requests initiated by administrative commands like CHECKPOINT. The counter accumulates since the last database cluster startup and provides insight into checkpoint activity levels.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_fetch_stat_checkpointer](pgstat_fetch_stat_checkpointer.md)
  - PG_RETURN_INT64
- Called from (representative examples):
  - SQL queries via pg_stat_get_checkpointer_num_requested() function
  - System monitoring and statistics collection

## Notes and Other Information
- Returns a 64-bit integer representing the cumulative count
- The counter is maintained in the checkpointer statistics structure's num_requested field
- This metric is useful for monitoring checkpoint frequency and database write activity
- Related to other checkpointer statistics functions for comprehensive checkpoint monitoring

## Simplified Source

```c
Datum
pg_stat_get_checkpointer_num_requested(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT64(pgstat_fetch_stat_checkpointer()->num_requested);
}
```