# AlterForeignDataWrapperOwner

## Location
[src/backend/commands/foreigncmds.c:286-323](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/foreigncmds.c#L286-L323)

## Overview
Changes the owner of a foreign data wrapper identified by name, performing catalog lookups and calling the internal ownership change logic.

## Definition
```c
ObjectAddress AlterForeignDataWrapperOwner(const char *name, Oid newOwnerId)
```

## Detailed Description
This function serves as the public interface for changing a foreign data wrapper's ownership when the wrapper is identified by name. It opens the pg_foreign_data_wrapper catalog table with exclusive row lock, searches for the named wrapper using the system cache, validates that the wrapper exists, and then delegates to AlterForeignDataWrapperOwner_internal() to perform the actual ownership change. The function properly manages resource cleanup by freeing the heap tuple and closing the relation before returning. It returns an ObjectAddress structure that can be used to reference the modified foreign data wrapper object.

## Parameters / Member Variables
- `name`: String name of the foreign data wrapper to modify
- `newOwnerId`: OID of the user who will become the new owner

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_foreign_data_wrapper (structure for catalog tuple data)
  - [table_open](../t/table_open.md) (opens catalog table with specified lock mode)
  - SearchSysCacheCopy1 (searches system cache and returns copy of tuple)
  - [CStringGetDatum](../C/CStringGetDatum.md) (converts C string to Datum)
  - HeapTupleIsValid (checks if tuple is valid)
  - [AlterForeignDataWrapperOwner_internal](AlterForeignDataWrapperOwner_internal.md) (performs the actual ownership change)
  - ObjectAddressSet (sets up ObjectAddress structure)
  - [heap_freetuple](../h/heap_freetuple.md) (frees memory allocated for tuple)
  - [table_close](../t/table_close.md) (closes catalog table and releases lock)
- Called from (representative examples):
  - [ExecAlterOwnerStmt](../E/ExecAlterOwnerStmt.md) (src/backend/commands/alter.c:844)

## Notes and Other Information
- Uses RowExclusiveLock to prevent concurrent modifications to the same foreign data wrapper
- Returns an ObjectAddress structure that identifies the modified object for dependency tracking
- Proper error handling with descriptive error messages when wrapper doesn't exist
- Memory management through heap_freetuple() and proper relation closing
- This is the name-based version of ownership change, complemented by AlterForeignDataWrapperOwner_oid() for OID-based changes
- Inherits all security restrictions from the internal function (superuser requirements)

## Simplified Source

```c
ObjectAddress
AlterForeignDataWrapperOwner(const char *name, Oid newOwnerId)
{
    // Open foreign data wrapper catalog with exclusive lock
    Relation rel = table_open(ForeignDataWrapperRelationId, RowExclusiveLock);

    // Look up wrapper by name in system cache
    HeapTuple tup = SearchSysCacheCopy1(FOREIGNDATAWRAPPERNAME, CStringGetDatum(name));

    if (!HeapTupleIsValid(tup))
        ereport(ERROR, "foreign-data wrapper \"%s\" does not exist", name);

    // Extract wrapper OID from tuple
    Form_pg_foreign_data_wrapper form = (Form_pg_foreign_data_wrapper) GETSTRUCT(tup);
    Oid fdwId = form->oid;

    // Delegate actual ownership change to internal function
    AlterForeignDataWrapperOwner_internal(rel, tup, newOwnerId);

    // Create return address
    ObjectAddress address;
    ObjectAddressSet(address, ForeignDataWrapperRelationId, fdwId);

    // Cleanup
    heap_freetuple(tup);
    table_close(rel, RowExclusiveLock);

    return address;
}
```