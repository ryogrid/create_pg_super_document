# pgstat_report_autovac

## Location
[src/backend/utils/activity/pgstat_database.c:55-80](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_database.c#L55-L80)

## Overview
Records the start time of an autovacuum process for a specific database in PostgreSQL's statistics system.

## Definition
```c
void pgstat_report_autovac(Oid dboid)
```

## Detailed Description
This function is called from the autovacuum process to report the startup of an autovacuum operation on a specific database. It updates the database's statistics to record when the last autovacuum started by setting the `last_autovac_time` field to the current timestamp. The function is designed to work before `InitPostgres` is completed, which is why it requires the database OID to be passed explicitly rather than relying on `MyDatabaseId`. The function uses a locked entry reference to ensure thread-safe access to the shared statistics data.

## Parameters / Member Variables
- `dboid`: The OID (Object Identifier) of the database for which autovacuum is starting

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_get_entry_ref_locked](pgstat_get_entry_ref_locked.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [pgstat_unlock_entry](pgstat_unlock_entry.md)
  - PGSTAT_KIND_DATABASE
  - [PgStat_EntryRef](../P/PgStat_EntryRef.md)
  - [PgStatShared_Database](../P/PgStatShared_Database.md)
- Called from (representative examples):
  - [AutoVacWorkerMain](../A/AutoVacWorkerMain.md) (in src/backend/postmaster/autovacuum.c:1549)

## Notes and Other Information
- This function is part of PostgreSQL's statistics collection system
- Located in src/backend/utils/activity/pgstat_database.c:55-80
- Must be called under the postmaster (not in single-user mode)
- Uses locked access to shared statistics to ensure consistency
- Reports the start of autovacuum instantly for consistency with end-of-vacuum reporting
- Called before InitPostgres completion, hence requires explicit database OID parameter

## Simplified Source

```c
void
pgstat_report_autovac(Oid dboid)
{
    PgStat_EntryRef *entry_ref;
    PgStatShared_Database *dbentry;

    // Must be under postmaster (not single-user mode)
    Assert(IsUnderPostmaster);

    // Get locked reference to database statistics entry
    entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_DATABASE,
                                            dboid, InvalidOid, false);

    // Update last autovacuum start time to current timestamp
    dbentry = (PgStatShared_Database *) entry_ref->shared_stats;
    dbentry->stats.last_autovac_time = GetCurrentTimestamp();

    // Release the lock
    pgstat_unlock_entry(entry_ref);
}
```