# pgstat_get_local_beentry_by_index

## Location
src/backend/utils/activity/backend_status.c: 1128 - 1147

## Overview
Retrieves a LocalPgBackendStatus entry by its 1-based index position in the localBackendStatusTable, providing access to backend status information with locally computed additions.

## Definition


## Detailed Description
This function provides access to LocalPgBackendStatus entries using a 1-based array index rather than a process number. It's similar to pgstat_get_beentry_by_proc_number() but returns the full LocalPgBackendStatus structure with locally computed additions like transaction IDs (xid and xmin). The function performs bounds checking to ensure the index is within the valid range (1 to localNumBackends) and returns the corresponding entry from the localBackendStatusTable array.

## Parameters / Member Variables
- `idx`: A 1-based index into the localBackendStatusTable (valid range: 1 to localNumBackends)

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_read_current_status
- Called from (representative examples):
  - PG_STAT_GET_PROGRESS_COLS
  - PG_STAT_GET_ACTIVITY_COLS
  - [pg_stat_get_db_numbackends](pg_stat_get_db_numbackends.md)

## Notes and Other Information
- Unlike pgstat_get_beentry_by_proc_number(), this function uses a 1-based array index rather than a ProcNumber
- The function performs bounds checking and returns NULL for out-of-range indices (though no current caller passes invalid indices)
- First calls pgstat_read_current_status() to ensure the local status table is up-to-date
- The caller is responsible for checking user permissions to view the returned information, especially query strings
- Used primarily by SQL-callable statistics functions that need to iterate through all backend entries
- Part of PostgreSQL's statistics collection system, enabling enumeration of all active backend processes
- Returns a direct pointer to the array element, providing efficient O(1) access when the index is known