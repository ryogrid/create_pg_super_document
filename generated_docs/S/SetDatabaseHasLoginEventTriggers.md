# SetDatabaseHasLoginEventTriggers

## Location
[src/backend/commands/event_trigger.c:386-422](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L386-L422)

## Overview
Sets the pg_database.dathasloginevt flag for the current database to indicate that the database has login event triggers defined.

## Definition

```c
void
SetDatabaseHasLoginEventTriggers(void)
```
## Detailed Description
This function updates the PostgreSQL system catalog to mark that the current database has login event triggers. It modifies the dathasloginevt flag in the pg_database system catalog table, which serves as an optimization hint for the system to know whether it needs to check for and potentially fire login event triggers when users connect to this database.

The function implements proper locking mechanisms to prevent conflicts with other operations that might be checking or modifying this flag. It uses a shared lock specifically designed to prevent conflicts with EventTriggerOnLogin() which might be trying to reset the same flag. The function performs an in-place update of the catalog tuple only if the flag is not already set, ensuring efficiency.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md) (to open the pg_database relation)
  - [LockSharedObject](../L/LockSharedObject.md) (to acquire exclusive lock preventing conflicts)
  - [SearchSysCacheLockedCopy1](SearchSysCacheLockedCopy1.md) (to find and lock the database tuple)
  - HeapTupleIsValid (to validate the found tuple)
  - GETSTRUCT (to extract the form structure from the tuple)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (to update the catalog tuple)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md) (to ensure visibility of changes)
  - [UnlockTuple](../U/UnlockTuple.md) (to release the tuple lock)
  - [table_close](../t/table_close.md) (to close the relation)
  - [heap_freetuple](../h/heap_freetuple.md) (to free the tuple memory)
- Called from (representative examples):
  - [insert_event_trigger_tuple](../i/insert_event_trigger_tuple.md)
  - [AlterEventTrigger](../A/AlterEventTrigger.md)

## Notes and Other Information
- The function uses a custom locking mechanism (LockSharedObject with AccessExclusiveLock) specifically to coordinate with EventTriggerOnLogin()
- The lock doesn't block database access or other operations; it's specifically for coordinating the dathasloginevt flag modifications
- Only updates the catalog if the flag is not already set, providing an optimization to avoid unnecessary writes
- The function operates on the current database context (MyDatabaseId)
- This flag serves as a performance optimization to avoid checking for login triggers when none exist in a database
- Memory management is handled properly with heap_freetuple() to prevent memory leaks

## Simplified Source

```c
void
SetDatabaseHasLoginEventTriggers(void)
{
    Form_pg_database db;
    Relation pg_db;
    ItemPointerData otid;
    HeapTuple tuple;

    // Open pg_database catalog with exclusive lock
    pg_db = table_open(DatabaseRelationId, RowExclusiveLock);

    // Acquire custom lock to prevent conflicts with EventTriggerOnLogin()
    LockSharedObject(DatabaseRelationId, MyDatabaseId, 0, AccessExclusiveLock);

    // Find and lock the current database tuple
    tuple = SearchSysCacheLockedCopy1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for database %u", MyDatabaseId);

    otid = tuple->t_self;
    db = (Form_pg_database) GETSTRUCT(tuple);

    // Set the login event trigger flag if not already set
    if (!db->dathasloginevt) {
        db->dathasloginevt = true;
        CatalogTupleUpdate(pg_db, &otid, tuple);
        CommandCounterIncrement();
    }

    // Cleanup and unlock
    UnlockTuple(pg_db, &otid, InplaceUpdateTupleLock);
    table_close(pg_db, RowExclusiveLock);
    heap_freetuple(tuple);
}
```