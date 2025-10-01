# pgstat_reset_database_timestamp

## Location
[src/backend/utils/activity/pgstat_database.c:354-374](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_database.c#L354-L374)

## Overview
pgstat_reset_database_timestamp is a function that updates the reset timestamp of a database's statistics without clearing the actual statistical data.

## Definition
void pgstat_reset_database_timestamp(Oid dboid, TimestampTz ts)

## Detailed Description
This function provides a mechanism to update only the reset timestamp of a database's statistics entry while preserving all the accumulated statistical data. It operates by obtaining a locked reference to the shared database statistics entry, updating the stat_reset_timestamp field with the provided timestamp, and then releasing the lock. This functionality is useful when you want to mark when statistics collection was conceptually "reset" without actually clearing the data, such as when responding to administrative commands that reset statistics views or when establishing new baseline timestamps for monitoring purposes.

## Parameters / Member Variables
- : Oid (Object Identifier) of the database whose reset timestamp should be updated
- : TimestampTz representing the new reset timestamp to be recorded

## Dependencies
- Functions called/Symbols referenced:
  - [PgStat_EntryRef](../P/PgStat_EntryRef.md) (statistics entry reference structure)
  - [PgStatShared_Database](../P/PgStatShared_Database.md) (shared database statistics structure)
  - [pgstat_get_entry_ref_locked](pgstat_get_entry_ref_locked.md) (obtains locked reference to statistics entry)
  - PGSTAT_KIND_DATABASE (constant indicating database-level statistics)
  - [pgstat_unlock_entry](pgstat_unlock_entry.md) (releases lock on statistics entry)
- Called from (representative examples):
  - [pgstat_reset](pgstat_reset.md) (from src/backend/utils/activity/pgstat.c:745)

## Notes and Other Information
- The function uses MyDatabaseId instead of the provided dboid parameter when calling pgstat_get_entry_ref_locked, which suggests it operates on the current database
- The function follows a lock-modify-unlock pattern to ensure thread-safe access to shared statistics data
- This is part of PostgreSQL's shared memory statistics system where multiple processes can safely access and modify statistics
- The reset timestamp is commonly used by monitoring tools and administrative functions to understand when statistics were last reset
- Unlike functions that clear statistics data, this function only updates metadata about when the reset conceptually occurred

## Simplified Source

```c
void pgstat_reset_database_timestamp(Oid dboid, TimestampTz ts) {
    // Get locked reference to database statistics entry
    PgStat_EntryRef *dbref = pgstat_get_entry_ref_locked(PGSTAT_KIND_DATABASE, MyDatabaseId, InvalidOid, false);

    // Update the reset timestamp in shared memory
    PgStatShared_Database *dbentry = (PgStatShared_Database *) dbref->shared_stats;
    dbentry->stats.stat_reset_timestamp = ts;

    // Release the lock
    pgstat_unlock_entry(dbref);
}
```