# pgstat_report_deadlock

## Location
[src/backend/utils/activity/pgstat_database.c:125-139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_database.c#L125-L139)

## Overview
Records the occurrence of a deadlock in PostgreSQL's database statistics system.

## Definition
```c
void pgstat_report_deadlock(void)
```

## Detailed Description
This function is called whenever a deadlock is detected in the PostgreSQL system to increment the deadlock counter for the current database. It provides a simple mechanism for tracking deadlock occurrences as part of the database's statistical information. The function operates only when statistics tracking is enabled and updates the deadlock count in the pending database statistics entry. This information is valuable for database administrators to monitor deadlock frequency and identify potential concurrency issues.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_prep_database_pending](pgstat_prep_database_pending.md)
  - [PgStat_StatDBEntry](../P/PgStat_StatDBEntry.md)
- Called from (representative examples):
  - [DeadLockReport](../D/DeadLockReport.md) (in src/backend/storage/lmgr/deadlock.c:1128)

## Notes and Other Information
- This function is part of PostgreSQL's statistics collection system
- Located in src/backend/utils/activity/pgstat_database.c:125-139
- Only operates when statistics tracking is enabled (pgstat_track_counts)
- Uses the current database ID (MyDatabaseId) to identify which database statistics to update
- Provides simple increment operation for deadlock counting
- Called by the deadlock detection mechanism when deadlocks are identified

## Simplified Source

```c
void
pgstat_report_deadlock(void)
{
    PgStat_StatDBEntry *dbent;

    // Only track statistics if enabled
    if (!pgstat_track_counts)
        return;

    // Get the database statistics entry for current database
    dbent = pgstat_prep_database_pending(MyDatabaseId);

    // Increment the deadlock counter
    dbent->deadlocks++;
}
```