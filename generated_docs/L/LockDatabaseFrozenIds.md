# LockDatabaseFrozenIds

## Location
[src/backend/storage/lmgr/lmgr.c:487-502](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L487-L502)

## Overview
Acquires an exclusive lock on the database's frozen transaction ID update mechanism to ensure only one backend per database can execute vac_update_datfrozenxid().

## Definition

```c
void
LockDatabaseFrozenIds(LOCKMODE lockmode)
```
## Detailed Description
This function provides a critical locking mechanism for database-wide transaction ID and MultiXact ID management. It ensures that only one backend per database can execute the vac_update_datfrozenxid() function, which updates the pg_database.datfrozenxid and pg_database.datminmxid values based on the minimum values found across all relations in the database.

The lock prevents race conditions that could cause datfrozenxid or datminmxid to move backward, which would be dangerous for transaction visibility and could lead to calling vac_truncate_clog() with inconsistent frozen ID values. This is essential for maintaining database consistency and preventing transaction ID wraparound issues.

## Parameters / Member Variables
- `lockmode`: The type of lock to acquire (typically ExclusiveLock for this operation)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_DATABASE_FROZEN_IDS (macro to set up lock tag for database frozen ID operations)
  - [LockAcquire](LockAcquire.md) (core lock acquisition function)
  - MyDatabaseId (global variable containing the current database OID)
- Called from (representative examples):
  - [vac_update_datfrozenxid](../v/vac_update_datfrozenxid.md) (vacuum function that updates database frozen transaction IDs)
  - [XLTW_Oper](../X/XLTW_Oper.md) (transaction lock wait operations)

## Notes and Other Information
- Uses LOCKTAG_DATABASE_FROZEN_IDS lock tag type, which is specific to database-level frozen ID operations
- The lock is scoped to a specific database using MyDatabaseId, allowing concurrent operations on different databases
- This lock is typically held during the entire duration of datfrozenxid/datminmxid calculation and update
- Critical for preventing transaction ID wraparound and maintaining database consistency
- The function is usually called with ExclusiveLock mode to ensure exclusive access
- Part of PostgreSQL's vacuum and transaction ID management infrastructure
- Helps coordinate with clog (commit log) and multixact truncation operations

## Simplified Source

```c
void
LockDatabaseFrozenIds(LOCKMODE lockmode)
{
    LOCKTAG tag;

    // Set up lock tag for database frozen ID operations
    SET_LOCKTAG_DATABASE_FROZEN_IDS(tag, MyDatabaseId);

    // Acquire the database-specific frozen ID lock
    (void) LockAcquire(&tag, lockmode, false, false);
}
```