# AlterForeignServerOwner

## Location
src/backend/commands/foreigncmds.c: 426 - 460

## Overview
Public interface function for changing a foreign server's owner by server name, providing name resolution and calling the internal implementation.

## Definition
```c
ObjectAddress AlterForeignServerOwner(const char *name, Oid newOwnerId)
```

## Detailed Description
This function serves as the public API for changing foreign server ownership when the server is identified by name rather than OID. It handles the name-to-OID resolution by searching the system catalog, validates that the server exists, and delegates the actual ownership change logic to the internal worker function. The function follows PostgreSQL's standard pattern for DDL operations by opening the catalog relation with appropriate locking, performing the operation, and returning an ObjectAddress for the modified object.

## Parameters / Member Variables
- `name`: String name of the foreign server whose ownership should be changed
- `newOwnerId`: Object ID of the new owner to be assigned to the foreign server

## Dependencies
- Functions called/Symbols referenced:
  - table_open: Open the pg_foreign_server catalog with exclusive lock
  - SearchSysCacheCopy1: Look up foreign server by name in system cache
  - CStringGetDatum: Convert C string to Datum for cache lookup
  - AlterForeignServerOwner_internal: Perform the actual ownership change
  - ObjectAddressSet: Create ObjectAddress for return value
  - heap_freetuple: Free the tuple memory after use
  - table_close: Close the catalog relation with lock release
- Called from (representative examples):
  - ExecAlterOwnerStmt: General ALTER OWNER statement execution
  - DEFREM_H: Header declaration for external usage

## Notes and Other Information
- Throws ERROR if the named foreign server does not exist
- Uses RowExclusiveLock on the catalog to prevent concurrent modifications
- Returns ObjectAddress pointing to the modified foreign server for further processing
- Properly manages memory by freeing the tuple after use
- Part of the standard DDL infrastructure for ownership changes
- Name-based lookup provides user-friendly interface compared to OID-based variants