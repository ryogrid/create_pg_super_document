# pgstat_fetch_stat_numbackends

## Location
[src/backend/utils/activity/backend_status.c:1148-1163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/backend_status.c#L1148-L1163)

## Overview
Returns the current number of active backend sessions tracked in the localBackendStatusTable, providing the maximum valid 1-based index for accessing backend entries.

## Definition

```c
int
pgstat_fetch_stat_numbackends(void)
```
## Detailed Description
This is a support function for PostgreSQL's SQL-callable pgstat* functions that returns the total count of sessions currently known in the localBackendStatusTable. The returned value represents the maximum valid 1-based index that can be passed to pgstat_get_local_beentry_by_index(). The function ensures the local status table is current by calling pgstat_read_current_status() before returning the count.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_read_current_status](pgstat_read_current_status.md)
- Called from (representative examples):
  - PG_STAT_GET_PROGRESS_COLS
  - PG_STAT_GET_ACTIVITY_COLS  
  - [pg_stat_get_db_numbackends](pg_stat_get_db_numbackends.md)

## Notes and Other Information
- Returns the value of localNumBackends, which represents the current count of active backend processes
- Always calls pgstat_read_current_status() first to ensure the count reflects the most current state
- The returned value can be used as the upper bound for iterating through backend entries using pgstat_get_local_beentry_by_index()
- Essential for SQL functions that need to enumerate all active backend sessions
- Part of PostgreSQL's statistics collection system, enabling monitoring of the total number of active database connections
- The count may change between calls as backends start and stop, so it should be retrieved fresh when needed