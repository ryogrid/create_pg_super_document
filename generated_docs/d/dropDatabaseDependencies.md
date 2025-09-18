# dropDatabaseDependencies

## Location
src/backend/catalog/pg_shdepend.c: 999 - 1046

## Overview
Removes all shared dependency entries associated with a database that is being dropped, cleaning up both dependencies owned by the database and dependencies on the database itself.

## Definition
```c
void dropDatabaseDependencies(Oid databaseId)
```

## Detailed Description
This function performs a comprehensive cleanup of shared dependencies when a database is being dropped. It operates in two phases: first, it removes all dependency entries where the database ID appears in the dbid field (representing objects within the database that depend on shared objects), and second, it removes entries where the database itself is the dependent object. This two-phase approach ensures complete cleanup of the dependency graph, preventing orphaned dependency records and maintaining referential integrity in the shared dependency system.

## Parameters / Member Variables
- `databaseId`: OID of the database being dropped whose dependencies need to be removed

## Dependencies
- Functions called/Symbols referenced:
  - [systable_beginscan](../s/systable_beginscan.md) (initiates system catalog scan)
  - [systable_getnext](../s/systable_getnext.md) (retrieves next tuple from scan)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md) (removes individual dependency tuples)
  - [shdepDropDependency](../s/shdepDropDependency.md) (removes dependencies on the database object itself)
  - [SysScanDesc](../S/SysScanDesc.md) (system catalog scan descriptor)
  - SHARED_DEPENDENCY_INVALID (dependency type constant)
- Called from (representative examples):
  - [dropdb](dropdb.md) (database deletion in dbcommands.c:1772)

## Notes and Other Information
- Uses RowExclusiveLock on SharedDependRelationId to ensure exclusive access during dependency cleanup
- First phase: scans SharedDependDependerIndexId using database ID to find all objects in the database that depend on shared objects
- Second phase: uses shdepDropDependency to remove dependencies where the database itself is the dependent object
- Ensures complete cleanup by addressing both directions of dependency relationships
- Critical for preventing dependency system corruption when databases are dropped
- Uses CatalogTupleDelete for efficient batch deletion of dependency tuples
- Part of the database drop process to maintain consistency in the shared object dependency graph