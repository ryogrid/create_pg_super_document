# pgstat_get_local_beentry_by_proc_number

## Location
src/backend/utils/activity/backend_status.c: 1097 - 1127

## Overview
Retrieves a LocalPgBackendStatus entry by process number, providing access to backend status information with locally computed additions like transaction ID and xmin values.

## Definition


## Detailed Description
This function is similar to pgstat_get_beentry_by_proc_number() but returns the full LocalPgBackendStatus structure instead of just the embedded PgBackendStatus. The LocalPgBackendStatus includes locally computed additions such as transaction IDs (xid) and xmin values of the backend process. The function uses binary search (bsearch()) to efficiently locate the desired entry in the sorted localBackendStatusTable array, taking advantage of the fact that entries are ordered by proc_number.

## Parameters / Member Variables
- `procNumber`: The ProcNumber of the desired backend session

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_read_current_status
  - bsearch (C standard library function)
  - cmp_lbestatus
  - ProcNumber (type)
  - LocalPgBackendStatus (struct type)
- Called from (representative examples):
  - pgstat_get_beentry_by_proc_number
  - PG_STAT_GET_SUBXACT_COLS

## Notes and Other Information
- Unlike pgstat_get_local_beentry_by_index(), this function takes a ProcNumber rather than an array index
- The function first calls pgstat_read_current_status() to ensure the local status table is up-to-date
- Uses binary search for O(log n) lookup efficiency, leveraging the sorted nature of localBackendStatusTable
- The caller is responsible for checking user permissions to view the returned information, especially query strings
- Returns NULL if the specified process number is not found in the table
- Part of PostgreSQL's statistics collection infrastructure, providing detailed backend process information including transaction state