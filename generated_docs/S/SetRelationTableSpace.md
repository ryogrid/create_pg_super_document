# SetRelationTableSpace

## Location
src/backend/commands/tablecmds.c: 3618 - 3662

## Overview
SetRelationTableSpace updates the tablespace and optionally the file number for a relation in the pg_class system catalog, handling the low-level catalog modification aspect of tablespace moves.

## Definition
```c
void SetRelationTableSpace(Relation rel, Oid newTableSpaceId, RelFileNumber newRelFilenumber)
```

## Detailed Description
This function performs the actual catalog update when moving a relation to a new tablespace. It modifies the pg_class system catalog to update the reltablespace field and optionally the relfilenode field. The function also handles dependency tracking for relations without physical storage by calling changeDependencyOnTablespace. It assumes validation has already been performed by CheckRelationTableSpaceMove and requires the caller to make changes visible with CommandCounterIncrement.

## Parameters / Member Variables
- `rel`: The relation being moved, must be held with AccessExclusiveLock
- `newTableSpaceId`: The OID of the destination tablespace
- `newRelFilenumber`: The new file number for the relation, or InvalidRelFileNumber if not being updated

## Dependencies
- Functions called/Symbols referenced:
  - CheckRelationTableSpaceMove
  - SearchSysCacheLockedCopy1
  - CatalogTupleUpdate
  - UnlockTuple
  - changeDependencyOnTablespace
  - heap_freetuple
  - RELKIND_HAS_STORAGE
  - RelFileNumberIsValid
- Called from (representative examples):
  - reindex_index
  - ATExecSetTableSpace
  - ATExecSetTableSpaceNoStorage

## Notes and Other Information
- The function asserts that CheckRelationTableSpaceMove returns true, ensuring validation was performed
- MyDatabaseTableSpace is stored as InvalidOid (0) in pg_class.reltablespace
- Dependency tracking is only updated for relations without physical storage (views, etc.)
- The caller is responsible for making the change visible and managing the overall transaction
- Uses row-exclusive lock on pg_class during the update operation