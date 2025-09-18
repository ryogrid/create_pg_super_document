# copyTemplateDependencies

## Location
src/backend/catalog/pg_shdepend.c: 895 - 998

## Overview
Creates the initial shared dependencies for a new database by copying dependency records from a template database, establishing proper relationships with shared objects like roles and tablespaces.

## Definition
```c
void copyTemplateDependencies(Oid templateDbId, Oid newDbId)
```

## Detailed Description
This function establishes the shared dependency relationships for a newly created database by scanning all dependency entries associated with the template database and creating corresponding entries for the new database. It performs an efficient batch insertion process using tuple slots to minimize the performance impact of inserting potentially large numbers of dependency records. The function specifically excludes copying dependencies with dbId == 0 (shared objects), which prevents copying the ownership dependency of the template database itself - a desired behavior to avoid inappropriate ownership relationships.

## Parameters / Member Variables
- `templateDbId`: OID of the template database from which to copy dependencies
- `newDbId`: OID of the new database that will receive the copied dependencies

## Dependencies
- Functions called/Symbols referenced:
  - MakeSingleTupleTableSlot (creates tuple slots for batch operations)
  - ExecDropSingleTupleTableSlot (cleans up tuple slots)
  - ExecClearTuple (clears tuple slot contents)
  - ExecStoreVirtualTuple (stores tuple data in slot)
  - CatalogOpenIndexes (opens catalog indexes for insertion)
  - CatalogCloseIndexes (closes catalog indexes)
  - CatalogTuplesMultiInsertWithInfo (performs batch tuple insertion)
  - systable_beginscan/systable_getnext (system catalog scanning functions)
  - Form_pg_shdepend (shared dependency tuple structure)
- Called from (representative examples):
  - createdb (database creation in dbcommands.c:1468)

## Notes and Other Information
- Uses batch insertion with configurable slot count based on MAX_CATALOG_MULTI_INSERT_BYTES to optimize performance
- Delays slot initialization until needed to avoid unnecessary memory allocation
- Copies all dependency fields except dbId, which is changed from templateDbId to newDbId
- Scans using SharedDependDependerIndexId for efficient template database dependency retrieval
- Excludes shared object dependencies (dbId == 0) to prevent inappropriate template database ownership copying
- Properly handles cleanup of allocated tuple slots to prevent memory leaks
- Uses RowExclusiveLock on the shared dependency relation during the copy operation
- Essential for database creation process to ensure new databases have proper relationships with shared objects