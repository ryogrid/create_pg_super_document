# RenameDatabase

## Location
[src/backend/commands/dbcommands.c:1863-1963](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L1863-L1963)

## Overview
RenameDatabase renames an existing PostgreSQL database by updating the database name in the system catalog while ensuring proper locking and validation checks.

## Definition

```c
struct dirent *xlde;
```
## Detailed Description
RenameDatabase performs a complete database rename operation with comprehensive safety checks. The function acquires an exclusive lock on the target database to prevent concurrent access, validates ownership and privileges, ensures the new name doesn't conflict with existing databases, and updates the database name in the pg_database system catalog. The operation includes special handling to prevent renaming the currently connected database and ensures no other active sessions are using the database during the rename process.

## Parameters / Member Variables
- : The current name of the database to be renamed
- : The desired new name for the database

## Dependencies
- Functions called/Symbols referenced:
  - [get_db_info](../g/get_db_info.md): Retrieves database information and acquires locks
  - [object_ownercheck](../o/object_ownercheck.md): Verifies database ownership permissions
  - [have_createdb_privilege](../h/have_createdb_privilege.md): Checks if user has database creation privileges
  - [get_database_oid](../g/get_database_oid.md): Looks up database OID by name
  - [CountOtherDBBackends](../C/CountOtherDBBackends.md): Counts active connections to the database
  - [SearchSysCacheLockedCopy1](../S/SearchSysCacheLockedCopy1.md): Retrieves and locks database catalog tuple
  - [namestrcpy](../n/namestrcpy.md): Copies the new name into the database tuple
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates the database catalog entry
  - InvokeObjectPostAlterHook: Triggers post-alter event hooks
- Called from (representative examples):
  - [ExecRenameStmt](../E/ExecRenameStmt.md): Statement execution handler for RENAME operations

## Notes and Other Information
- Requires AccessExclusiveLock on the database to prevent concurrent operations
- Cannot rename the currently connected database (MyDatabaseId check)
- Validates that no other sessions are actively using the database
- Includes regression testing name validation when built with appropriate flags
- Returns ObjectAddress pointing to the renamed database for dependency tracking
- Maintains lock until transaction commit to ensure consistency

## Simplified Source

```c
ObjectAddress RenameDatabase(const char *oldname, const char *newname)
{
    // Open database catalog and find the target database
    Relation rel = table_open(DatabaseRelationId, RowExclusiveLock);
    Oid db_id;

    if (!get_db_info(oldname, AccessExclusiveLock, &db_id, NULL, NULL, NULL,
                     NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE),
                errmsg("database \"%s\" does not exist", oldname)));

    // Validate permissions
    if (!object_ownercheck(DatabaseRelationId, db_id, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_DATABASE, oldname);

    if (!have_createdb_privilege())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to rename database")));

    // Validate rename constraints
    validate_database_rename_constraints(db_id, oldname, newname);

    // Perform the actual rename
    HeapTuple newtup = SearchSysCacheLockedCopy1(DATABASEOID,
                                                 ObjectIdGetDatum(db_id));
    if (!HeapTupleIsValid(newtup))
        elog(ERROR, "cache lookup failed for database %u", db_id);

    ItemPointerData otid = newtup->t_self;
    namestrcpy(&(((Form_pg_database) GETSTRUCT(newtup))->datname), newname);
    CatalogTupleUpdate(rel, &otid, newtup);
    UnlockTuple(rel, &otid, InplaceUpdateTupleLock);

    // Fire hooks and build return value
    InvokeObjectPostAlterHook(DatabaseRelationId, db_id, 0);

    ObjectAddress address;
    ObjectAddressSet(address, DatabaseRelationId, db_id);

    table_close(rel, NoLock);
    return address;
}

static void validate_database_rename_constraints(Oid db_id,
                                                 const char *oldname,
                                                 const char *newname)
{
    // Check for name conflicts
    if (OidIsValid(get_database_oid(newname, true)))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_DATABASE),
                errmsg("database \"%s\" already exists", newname)));

    // Cannot rename current database
    if (db_id == MyDatabaseId)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("current database cannot be renamed")));

    // Check for active sessions
    int notherbackends, npreparedxacts;
    if (CountOtherDBBackends(db_id, &notherbackends, &npreparedxacts))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
                errmsg("database \"%s\" is being accessed by other users", oldname),
                errdetail_busy_db(notherbackends, npreparedxacts)));
}
```