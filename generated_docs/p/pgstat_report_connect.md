# pgstat_report_connect

## Location
[src/backend/utils/activity/pgstat_database.c:191-207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_database.c#L191-L207)

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
  - [pgstat_should_report_connstat](pgstat_should_report_connstat.md)
  - [pgstat_prep_database_pending](pgstat_prep_database_pending.md)
  - [PgStat_StatDBEntry](../P/PgStat_StatDBEntry.md) (data structure)
  - MyDatabaseId (global variable)
  - MyStartTimestamp (global variable)
  - pgLastSessionReportTime (global variable)
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md) (in src/backend/tcop/postgres.c:4354)

## Notes and Other Information
- Only reports statistics for normal backend processes (B_BACKEND), not parallel workers or WAL senders
- Updates pgLastSessionReportTime to MyStartTimestamp for session duration tracking
- Uses pending statistics approach for efficient batch updates to shared memory
- Part of PostgreSQL's database-level session monitoring system
- The dboid parameter is provided but the function uses MyDatabaseId for consistency
- [Session](../S/Session.md) statistics help administrators monitor database connection patterns and usage

## Simplified Source

```c
// Simplified version of pgstat_report_connect
void pgstat_report_connect(Oid dboid) {
    // Check if we should report connection stats (only for regular backends)
    if (!pgstat_should_report_connstat()) {
        return;
    }

    // Record when this session started
    pgLastSessionReportTime = MyStartTimestamp;

    // Get database statistics entry and increment session counter
    PgStat_StatDBEntry *dbentry = pgstat_prep_database_pending(MyDatabaseId);
    dbentry->sessions++;
}
```

Key simplifications made:
- Added clear comments explaining each step
- Consolidated variable declaration with assignment
- Focused on the main execution path
- Removed unnecessary blank lines for better readability