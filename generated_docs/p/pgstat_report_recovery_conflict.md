# pgstat_report_recovery_conflict

## Location
[src/backend/utils/activity/pgstat_database.c:81-124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_database.c#L81-L124)

## Overview
Records and tracks different types of recovery conflicts that occur during Hot Standby operations in PostgreSQL.

## Definition
```c
void pgstat_report_recovery_conflict(int reason)
```

## Detailed Description
This function is responsible for tracking and recording various types of recovery conflicts that occur during Hot Standby replication. When a recovery conflict happens (where standby operations conflict with recovery activities), this function increments the appropriate counter in the database statistics entry based on the specific type of conflict. The function handles multiple conflict types including tablespace conflicts, lock conflicts, snapshot conflicts, buffer pin conflicts, logical slot conflicts, and startup deadlocks. Database-level conflicts are explicitly not counted since the database information is dropped upon replication.

## Parameters / Member Variables
- `reason`: An integer constant indicating the type of recovery conflict that occurred

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_prep_database_pending](pgstat_prep_database_pending.md)
  - [PgStat_StatDBEntry](../P/PgStat_StatDBEntry.md)
  - PROCSIG_RECOVERY_CONFLICT_DATABASE
  - PROCSIG_RECOVERY_CONFLICT_TABLESPACE
  - PROCSIG_RECOVERY_CONFLICT_LOCK
  - PROCSIG_RECOVERY_CONFLICT_SNAPSHOT
  - PROCSIG_RECOVERY_CONFLICT_BUFFERPIN
  - PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT
  - PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK
- Called from (representative examples):
  - [ProcessRecoveryConflictInterrupt](../P/ProcessRecoveryConflictInterrupt.md) (in src/backend/tcop/postgres.c:3193, 3212)

## Notes and Other Information
- This function is part of PostgreSQL's Hot Standby statistics tracking system
- Located in src/backend/utils/activity/pgstat_database.c:81-124
- Only operates when statistics tracking is enabled (pgstat_track_counts)
- Must be called under the postmaster (not in single-user mode)
- Database conflicts are intentionally not counted due to immediate database information removal during replication
- Each conflict type corresponds to a specific counter in the database statistics entry

## Simplified Source

```c
void pgstat_report_recovery_conflict(int reason)
{
    PgStat_StatDBEntry *dbentry;

    // Only track if we're under postmaster and statistics tracking is enabled
    Assert(IsUnderPostmaster);
    if (!pgstat_track_counts)
        return;

    // Get database statistics entry for current database
    dbentry = pgstat_prep_database_pending(MyDatabaseId);

    // Increment appropriate conflict counter based on reason
    switch (reason)
    {
        case PROCSIG_RECOVERY_CONFLICT_DATABASE:
            // Database conflicts not counted - info dropped on replication
            break;
        case PROCSIG_RECOVERY_CONFLICT_TABLESPACE:
            dbentry->conflict_tablespace++;
            break;
        case PROCSIG_RECOVERY_CONFLICT_LOCK:
            dbentry->conflict_lock++;
            break;
        case PROCSIG_RECOVERY_CONFLICT_SNAPSHOT:
            dbentry->conflict_snapshot++;
            break;
        case PROCSIG_RECOVERY_CONFLICT_BUFFERPIN:
            dbentry->conflict_bufferpin++;
            break;
        case PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT:
            dbentry->conflict_logicalslot++;
            break;
        case PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK:
            dbentry->conflict_startup_deadlock++;
            break;
    }
}
```