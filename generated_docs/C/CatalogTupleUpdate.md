# CatalogTupleUpdate

## Location
src/backend/catalog/indexing.c: 313 - 336

## Overview
CatalogTupleUpdate is a convenience function that updates a single tuple in a system catalog relation by replacing the tuple identified by an item pointer with new data, maintaining all associated indexes.

## Definition
```c
void CatalogTupleUpdate(Relation heapRel, ItemPointer otid, HeapTuple tup)
```

## Detailed Description
CatalogTupleUpdate provides a simplified interface for updating single tuples in system catalog relations. It handles the complete update process including constraint checking, heap tuple replacement, and index maintenance. The function internally manages the catalog index state by opening and closing indexes around the update operation, making it suitable for single-tuple operations but inefficient for bulk updates.

The function performs constraint validation before the update and ensures all catalog indexes remain consistent with the new tuple data. It uses the TU_All flag to indicate that all indexes should be updated during the operation.

## Parameters / Member Variables
- `heapRel`: The system catalog relation containing the tuple to be updated
- `otid`: ItemPointer identifying the specific tuple to be updated
- `tup`: The new HeapTuple data that will replace the existing tuple

## Dependencies
- Functions called/Symbols referenced:
  - CatalogIndexState
  - TU_UpdateIndexes
  - TU_All
  - CatalogTupleCheckConstraints
  - CatalogOpenIndexes
  - simple_heap_update
  - CatalogIndexInsert
  - CatalogCloseIndexes
- Called from (representative examples):
  - SetDefaultACL
  - ExecGrant_Attribute
  - ExecGrant_Relation
  - RemoveAttributeById
  - RelationClearMissing
  - index_concurrently_swap
  - AggregateCreate
  - StoreAttrDefault
  - AlterConstraintNamespaces
  - changeDependencyFor

## Notes and Other Information
- This is a convenience routine optimized for updating single catalog tuples
- Should not be used for multiple tuple updates due to the overhead of opening/closing indexes repeatedly
- For bulk operations, use CatalogTupleUpdateWithInfo instead, which accepts pre-opened index state
- The function performs complete constraint checking to maintain catalog integrity
- All catalog indexes are updated using the TU_All flag to ensure consistency
- Widely used throughout PostgreSQL DDL operations for modifying system catalog metadata
- The function automatically handles the index state lifecycle, making it easy to use but potentially inefficient for bulk operations