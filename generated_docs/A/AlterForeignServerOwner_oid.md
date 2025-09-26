# AlterForeignServerOwner_oid

## Location
[src/backend/commands/foreigncmds.c:461-485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/foreigncmds.c#L461-L485)

## Overview
OID-based interface function for changing a foreign server's owner, providing direct object ID lookup without name resolution.

## Definition
```c
void AlterForeignServerOwner_oid(Oid srvId, Oid newOwnerId)
```

## Detailed Description
This function provides an OID-based alternative to the name-based foreign server ownership change function. It directly looks up the foreign server by its object identifier rather than requiring name resolution, making it more efficient for internal operations where the OID is already known. The function follows the same general pattern as its name-based counterpart but skips the name-to-OID conversion step and delegates to the same internal worker function for the actual ownership change logic.

## Parameters / Member Variables
- `srvId`: Object ID of the foreign server whose ownership should be changed
- `newOwnerId`: Object ID of the new owner to be assigned to the foreign server

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md): Open the pg_foreign_server catalog with exclusive lock
  - SearchSysCacheCopy1: Look up foreign server by OID in system cache
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md): Convert OID to Datum for cache lookup
  - [AlterForeignServerOwner_internal](AlterForeignServerOwner_internal.md): Perform the actual ownership change
  - [heap_freetuple](../h/heap_freetuple.md): Free the tuple memory after use
  - [table_close](../t/table_close.md): Close the catalog relation with lock release
- Called from (representative examples):
  - [shdepReassignOwned_Owner](../s/shdepReassignOwned_Owner.md): Bulk ownership reassignment during role operations
  - DEFREM_H: Header declaration for external usage

## Notes and Other Information
- Returns void unlike the name-based version which returns ObjectAddress
- More efficient than name-based lookup when OID is already available
- Commonly used in internal operations and dependency management
- Throws ERROR if the foreign server OID does not exist
- Uses RowExclusiveLock on the catalog to prevent concurrent modifications
- Part of PostgreSQL's shared dependency management infrastructure
- Primarily used during role reassignment and cleanup operations