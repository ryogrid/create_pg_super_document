# pgstat_fetch_stat_wal

## Location
[src/backend/utils/activity/pgstat_wal.c:67-81](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_wal.c#L67-L81)

## Overview
Retrieves a pointer to the current WAL statistics structure, serving as a support function for SQL-callable pgstat* functions that need access to WAL usage data.

## Definition
PgStat_WalStats *pgstat_fetch_stat_wal(void)

## Detailed Description
This function provides access to the current snapshot of WAL (Write-Ahead Log) statistics by first ensuring the WAL statistics snapshot is up-to-date, then returning a pointer to the local WAL statistics structure. It acts as an interface between the internal statistics collection system and the SQL-accessible statistics functions.

The function follows the standard PostgreSQL statistics pattern of taking a snapshot of the current statistics before returning the data, ensuring consistency and avoiding race conditions when multiple processes are updating statistics concurrently.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_snapshot_fixed
  - PGSTAT_KIND_WAL
- Called from (representative examples):
  - PG_STAT_GET_WAL_COLS
  - pgstat_count_buffer_hit

## Notes and Other Information
- Returns a pointer to the WAL statistics structure in the local statistics snapshot (pgStatLocal.snapshot.wal)
- The function ensures thread-safety by taking a snapshot before returning data
- Used primarily by SQL functions that expose WAL statistics to users through system views
- Part of PostgreSQL's comprehensive statistics collection framework
- The returned pointer should not be modified as it points to shared statistics data