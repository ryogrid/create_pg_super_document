# pgstat_fetch_slru

## Location
[src/backend/utils/activity/pgstat_slru.c:105-117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_slru.c#L105-L117)

## Overview
Retrieves a snapshot of SLRU (Simple LRU) buffer cache statistics for SQL-callable pgstat functions.

## Definition

```c
PgStat_SLRUStats *
pgstat_fetch_slru(void)
```
## Detailed Description
This function serves as a support function for SQL-callable pgstat* functions that need to access SLRU statistics. It creates a snapshot of the current SLRU statistics and returns a pointer to the statistics structure. The function ensures that the statistics are up-to-date by taking a snapshot from the statistics collector before returning the data.

The function is designed to provide a consistent view of SLRU statistics at a specific point in time, which is important for SQL functions that expose these statistics to users through system views or direct function calls.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_snapshot_fixed](pgstat_snapshot_fixed.md)
  - PGSTAT_KIND_SLRU
- Called from (representative examples):
  - PG_STAT_GET_SLRU_COLS
  - pgstat_count_buffer_hit

## Notes and Other Information
- This function provides the interface between the statistics collection system and SQL-level access to SLRU statistics
- The returned pointer points to a snapshot of statistics, ensuring consistency during read operations
- Used by system views and functions that expose SLRU performance metrics to database administrators
- The function takes a snapshot specifically for SLRU statistics (PGSTAT_KIND_SLRU) rather than all statistics
- Part of the PostgreSQL statistics collector infrastructure for exposing internal metrics to SQL interfaces

## Simplified Source

```c
PgStat_SLRUStats *pgstat_fetch_slru(void) {
    // Take a snapshot of SLRU statistics
    pgstat_snapshot_fixed(PGSTAT_KIND_SLRU);

    // Return pointer to the local SLRU statistics snapshot
    return pgStatLocal.snapshot.slru;
}
```