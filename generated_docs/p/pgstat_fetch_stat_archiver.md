# pgstat_fetch_stat_archiver

## Location
[src/backend/utils/activity/pgstat_archiver.c:58-65](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_archiver.c#L58-L65)

## Overview
Retrieves a snapshot of archiver statistics from shared memory and returns a pointer to the local archiver statistics structure for SQL-accessible functions.

## Definition
```c
PgStat_ArchiverStats *pgstat_fetch_stat_archiver(void)
```

## Detailed Description
This function serves as a support function for SQL-callable pgstat functions that need to access archiver statistics. It first creates a snapshot of the current archiver statistics from shared memory using the pgstat snapshot mechanism, then returns a pointer to the local copy of the archiver statistics. This ensures that SQL functions get a consistent view of the statistics that won't change during their execution.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_snapshot_fixed](pgstat_snapshot_fixed.md)
  - PGSTAT_KIND_ARCHIVER
- Called from (representative examples):
  - [pg_stat_get_archiver](pg_stat_get_archiver.md)

## Notes and Other Information
This function is specifically designed to support PostgreSQL's SQL-level statistics access functions. The snapshot mechanism ensures that the returned statistics represent a consistent point-in-time view, preventing inconsistencies that could arise if the underlying shared memory statistics were updated during SQL function execution.