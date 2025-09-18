# pgstat_report_connect

## Location
src/backend/utils/activity/pgstat_database.c: 191 - 207

## Overview
Reports a new database connection to PostgreSQL's statistics system by incrementing the session counter for the current database.

## Definition
```c
void pgstat_report_connect(Oid dboid)
```

## Detailed Description
This function notifies the statistics system when a new connection to a database is established. It increments the session counter in the database statistics and records the session start time. The function only reports connection statistics for normal backend processes, excluding parallel workers and WAL sender processes to avoid skewing the session statistics.

The function first checks if connection statistics should be reported using pgstat_should_report_connstat(), which ensures only regular user backend connections are counted. It then updates the global pgLastSessionReportTime to track when the session started and increments the sessions counter in the database's pending statistics entry.

## Parameters / Member Variables
- `dboid`: The OID of the database being connected to (though the function actually uses MyDatabaseId for the statistics update)

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_should_report_connstat
  - pgstat_prep_database_pending
  - PgStat_StatDBEntry (data structure)
  - MyDatabaseId (global variable)
  - MyStartTimestamp (global variable)
  - pgLastSessionReportTime (global variable)
- Called from (representative examples):
  - PostgresMain (in src/backend/tcop/postgres.c:4354)

## Notes and Other Information
- Only reports statistics for normal backend processes (B_BACKEND), not parallel workers or WAL senders
- Updates pgLastSessionReportTime to MyStartTimestamp for session duration tracking
- Uses pending statistics approach for efficient batch updates to shared memory
- Part of PostgreSQL's database-level session monitoring system
- The dboid parameter is provided but the function uses MyDatabaseId for consistency
- Session statistics help administrators monitor database connection patterns and usage