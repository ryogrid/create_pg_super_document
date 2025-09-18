# pg_nextoid

## Location
src/backend/catalog/catalog.c: 616 - 694

## Overview
A SQL callable interface for GetNewOidWithIndex() that generates the next available OID for a specified column in a system catalog table, primarily used during initdb and corruption recovery scenarios.

## Definition
```c
Datum pg_nextoid(PG_FUNCTION_ARGS)
```

## Detailed Description
The `pg_nextoid` function provides a SQL-accessible way to obtain a new unique OID for a specified column in system catalog tables. It is designed as a specialized function that should rarely be needed outside of PostgreSQL initialization (initdb) and recovery from database corruption scenarios. The function enforces strict security and validation requirements:

- Requires superuser privileges to execute
- Only works on system catalog relations (not user tables)
- Validates that the specified index belongs to the specified table
- Ensures the target column is of type OID
- Verifies the index is appropriate for the specified column

The function is intentionally not documented in user-facing documentation as it is meant for internal PostgreSQL operations and emergency recovery situations.

## Parameters / Member Variables
- `reloid` (PG_GETARG_OID(0)): OID of the relation (table) to generate a new OID for
- `attname` (PG_GETARG_NAME(1)): Name of the attribute/column that needs a new OID value
- `idxoid` (PG_GETARG_OID(2)): OID of the index that should be used for uniqueness checking

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME: Extract name argument from function call
  - superuser: Check if current user has superuser privileges
  - table_open: Open relation with specified lock mode
  - [index_open](../i/index_open.md): Open index with specified lock mode
  - [IsSystemRelation](../I/IsSystemRelation.md): Verify if relation is a system catalog
  - [SearchSysCacheAttName](../S/SearchSysCacheAttName.md): Look up attribute information by name
  - IndexRelationGetNumberOfKeyAttributes: Get number of key attributes in index
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md): Generate new unique OID using specified index
  - [index_close](../i/index_close.md): Close index and release lock
  - table_close: Close relation and release lock
  - PG_RETURN_OID: Return OID value to SQL caller
- Called from (representative examples):
  - No direct references found (function is accessible via SQL interface)

## Notes and Other Information
- This function is restricted to superusers only as a security measure
- Only works on system catalog tables, not user-defined tables
- Performs extensive validation to ensure proper usage and prevent data corruption
- Uses RowExclusiveLock when opening relations and indexes to prevent concurrent modifications
- The function is designed for rare usage scenarios and emergency situations
- Implementation includes comprehensive error handling with specific error codes and messages
- Located in src/backend/catalog/catalog.c:616-694