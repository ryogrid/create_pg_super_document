# AlterDatabaseOwner

## Location
[src/backend/commands/dbcommands.c:2624-2736](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L2624-L2736)

## Overview
AlterDatabaseOwner implements the ALTER DATABASE name OWNER TO newowner command, which changes the ownership of a database to a new user while handling all necessary permission checks and dependency updates.

## Definition

```c
ObjectAddress
AlterDatabaseOwner(const char *dbname, Oid newOwnerId)
```
## Detailed Description
This function handles the SQL command `ALTER DATABASE name OWNER TO newowner` which transfers ownership of a database to a new user. The function:

1. Opens the pg_database catalog and searches for the specified database by name
2. Checks if the new owner is different from the current owner (no-op if same)
3. Verifies that the current user owns the database (required for ownership transfer)
4. Ensures the current user can assume the role of the new owner using check_can_set_role()
5. Verifies the current user has CREATEDB privileges (required for database ownership operations)
6. Updates the datdba field in pg_database to the new owner's OID
7. Handles ACL (access control list) updates by calling aclnewowner() if the database has a non-null datacl
8. Updates the shared dependency system to reflect the ownership change via changeDependencyOnOwner()
9. Triggers post-alter hooks and returns the database's ObjectAddress

The function ensures proper privilege checking and maintains referential integrity in PostgreSQL's dependency tracking system.

## Parameters / Member Variables
- `dbname`: Name of the database whose ownership is being changed
- `newOwnerId`: OID of the user who will become the new owner

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [check_can_set_role](../c/check_can_set_role.md)
  - [have_createdb_privilege](../h/have_createdb_privilege.md)
  - [LockTuple](../L/LockTuple.md)/UnlockTuple
  - [heap_getattr](../h/heap_getattr.md)
  - [aclnewowner](../a/aclnewowner.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [changeDependencyOnOwner](../c/changeDependencyOnOwner.md)
  - InvokeObjectPostAlterHook
  - ObjectAddressSet
- Called from (representative examples):
  - [ExecAlterOwnerStmt](../E/ExecAlterOwnerStmt.md)

## Notes and Other Information
- Requires current database ownership to execute the command
- Requires CREATEDB privilege from the current user (unlike other ALTER OWNER commands that check the destination owner)
- Uses InplaceUpdateTupleLock for atomic catalog updates
- Handles ACL inheritance properly when transferring ownership
- Updates PostgreSQL's shared dependency system to track the new ownership relationship
- No-op if the new owner is the same as the current owner (consistent with other PostgreSQL objects)
- Part of PostgreSQL's ownership management system integrated with the dependency tracking framework

## Simplified Source

```c
ObjectAddress
AlterDatabaseOwner(const char *dbname, Oid newOwnerId)
{
    // Look up database by name in pg_database catalog
    Relation rel = table_open(DatabaseRelationId, RowExclusiveLock);
    ScanKeyData scankey;
    ScanKeyInit(&scankey, Anum_pg_database_datname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(dbname));

    SysScanDesc scan = systable_beginscan(rel, DatabaseNameIndexId, true, NULL, 1, &scankey);
    HeapTuple tuple = systable_getnext(scan);

    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, "database \"%s\" does not exist", dbname);

    Form_pg_database datForm = (Form_pg_database) GETSTRUCT(tuple);
    Oid db_id = datForm->oid;

    // If new owner same as current, just return
    if (datForm->datdba != newOwnerId) {
        // Permission checks
        if (!object_ownercheck(DatabaseRelationId, db_id, GetUserId()))
            aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_DATABASE, dbname);

        check_can_set_role(GetUserId(), newOwnerId);

        // Must have CREATEDB privilege
        if (!have_createdb_privilege())
            ereport(ERROR, "permission denied to change owner of database");

        // Update database owner in catalog
        LockTuple(rel, &tuple->t_self, InplaceUpdateTupleLock);

        Datum repl_val[Natts_pg_database];
        bool repl_null[Natts_pg_database] = {0};
        bool repl_repl[Natts_pg_database] = {0};

        repl_repl[Anum_pg_database_datdba - 1] = true;
        repl_val[Anum_pg_database_datdba - 1] = ObjectIdGetDatum(newOwnerId);

        // Update ACL if present
        Datum aclDatum = heap_getattr(tuple, Anum_pg_database_datacl, RelationGetDescr(rel), &isNull);
        if (!isNull) {
            Acl *newAcl = aclnewowner(DatumGetAclP(aclDatum), datForm->datdba, newOwnerId);
            repl_repl[Anum_pg_database_datacl - 1] = true;
            repl_val[Anum_pg_database_datacl - 1] = PointerGetDatum(newAcl);
        }

        HeapTuple newtuple = heap_modify_tuple(tuple, RelationGetDescr(rel), repl_val, repl_null, repl_repl);
        CatalogTupleUpdate(rel, &newtuple->t_self, newtuple);
        UnlockTuple(rel, &tuple->t_self, InplaceUpdateTupleLock);

        heap_freetuple(newtuple);

        // Update ownership dependency
        changeDependencyOnOwner(DatabaseRelationId, db_id, newOwnerId);
    }

    InvokeObjectPostAlterHook(DatabaseRelationId, db_id, 0);

    ObjectAddress address;
    ObjectAddressSet(address, DatabaseRelationId, db_id);

    systable_endscan(scan);
    table_close(rel, NoLock);

    return address;
}
```