# CatalogTupleUpdate

## Location
[src/backend/catalog/indexing.c:313-336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/indexing.c#L313-L336)

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
  - [CatalogIndexState](CatalogIndexState.md)
  - TU_UpdateIndexes
  - TU_All
  - [CatalogTupleCheckConstraints](CatalogTupleCheckConstraints.md)
  - [CatalogOpenIndexes](CatalogOpenIndexes.md)
  - [simple_heap_update](../s/simple_heap_update.md)
  - [CatalogIndexInsert](CatalogIndexInsert.md)
  - [CatalogCloseIndexes](CatalogCloseIndexes.md)
- Called from (representative examples):
  - [SetDefaultACL](../S/SetDefaultACL.md)
  - [ExecGrant_Attribute](../E/ExecGrant_Attribute.md)
  - [ExecGrant_Relation](../E/ExecGrant_Relation.md)
  - [RemoveAttributeById](../R/RemoveAttributeById.md)
  - [RelationClearMissing](../R/RelationClearMissing.md)
  - [index_concurrently_swap](../i/index_concurrently_swap.md)
  - [AggregateCreate](../A/AggregateCreate.md)
  - [StoreAttrDefault](../S/StoreAttrDefault.md)
  - [AlterConstraintNamespaces](../A/AlterConstraintNamespaces.md)
  - [changeDependencyFor](../c/changeDependencyFor.md)

## Notes and Other Information
- This is a convenience routine optimized for updating single catalog tuples
- Should not be used for multiple tuple updates due to the overhead of opening/closing indexes repeatedly
- For bulk operations, use CatalogTupleUpdateWithInfo instead, which accepts pre-opened index state
- The function performs complete constraint checking to maintain catalog integrity
- All catalog indexes are updated using the TU_All flag to ensure consistency
- Widely used throughout PostgreSQL DDL operations for modifying system catalog metadata
- The function automatically handles the index state lifecycle, making it easy to use but potentially inefficient for bulk operations

## Simplified Source

```c
void
CatalogTupleUpdate(Relation heapRel, ItemPointer otid, HeapTuple tup)
{
    CatalogIndexState indstate;
    TU_UpdateIndexes updateIndexes = TU_All;

    // Check constraints on the new tuple
    CatalogTupleCheckConstraints(heapRel, tup);

    // Open all indexes for this catalog
    indstate = CatalogOpenIndexes(heapRel);

    // Update the heap tuple
    simple_heap_update(heapRel, otid, tup, &updateIndexes);

    // Update all associated indexes
    CatalogIndexInsert(indstate, tup, updateIndexes);

    // Clean up index state
    CatalogCloseIndexes(indstate);
}
```