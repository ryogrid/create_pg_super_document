# AlterForeignDataWrapperOwner_oid

## Location
[src/backend/commands/foreigncmds.c:324-348](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/foreigncmds.c#L324-L348)

## Overview
Changes the owner of a foreign data wrapper identified by OID, providing an alternative interface to the name-based ownership change function.

## Definition
```c
void AlterForeignDataWrapperOwner_oid(Oid fwdId, Oid newOwnerId)
```

## Detailed Description
This function serves as an OID-based alternative to AlterForeignDataWrapperOwner() for changing foreign data wrapper ownership. It opens the pg_foreign_data_wrapper catalog table with exclusive row lock, searches for the wrapper by OID using the system cache, validates the wrapper exists, and delegates to AlterForeignDataWrapperOwner_internal() to perform the actual ownership change. This function is particularly useful in contexts where the wrapper's OID is already known, such as during shared dependency operations or cascading ownership changes. Unlike the name-based version, it doesn't return an ObjectAddress since the caller already has the OID.

## Parameters / Member Variables
- `fwdId`: OID of the foreign data wrapper to modify
- `newOwnerId`: OID of the user who will become the new owner

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md) (opens catalog table with specified lock mode)
  - SearchSysCacheCopy1 (searches system cache and returns copy of tuple)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (converts OID to Datum for cache search)
  - HeapTupleIsValid (checks if tuple is valid)
  - [AlterForeignDataWrapperOwner_internal](AlterForeignDataWrapperOwner_internal.md) (performs the actual ownership change)
  - [heap_freetuple](../h/heap_freetuple.md) (frees memory allocated for tuple)
  - [table_close](../t/table_close.md) (closes catalog table and releases lock)
- Called from (representative examples):
  - [shdepReassignOwned_Owner](../s/shdepReassignOwned_Owner.md) (src/backend/catalog/pg_shdepend.c:1687)

## Notes and Other Information
- Uses RowExclusiveLock to prevent concurrent modifications, consistent with the name-based version
- Returns void rather than ObjectAddress since the caller already has the OID
- Provides more specific error message including the OID when wrapper doesn't exist
- Primarily used by shared dependency management code when reassigning ownership
- Complements AlterForeignDataWrapperOwner() by offering OID-based lookup instead of name-based
- Inherits all security restrictions from the internal function (superuser requirements)
- Proper resource management with heap_freetuple() and table_close() calls