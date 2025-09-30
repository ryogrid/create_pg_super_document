# CatalogTupleUpdateWithInfo

## Location
[src/backend/catalog/indexing.c:337-364](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/indexing.c#L337-L364)

## Overview
CatalogTupleUpdateWithInfo updates a tuple in a system catalog relation using caller-supplied index information, optimizing performance for bulk operations by amortizing index management overhead across multiple updates.

## Definition
```c
void CatalogTupleUpdateWithInfo(Relation heapRel, ItemPointer otid, HeapTuple tup, CatalogIndexState indstate)
```

## Detailed Description
CatalogTupleUpdateWithInfo is the optimized version of catalog tuple updating that accepts pre-opened index state from the caller. This design pattern improves performance when performing multiple catalog updates by avoiding the repeated overhead of opening and closing catalog indexes. The function performs constraint checking, updates the heap tuple, and maintains all associated indexes using the provided index state.

Like other catalog functions with "WithInfo" suffix, this function is designed for scenarios where it's important to amortize the cost of CatalogOpenIndexes/CatalogCloseIndexes operations across multiple updates. PostgreSQL may cache CatalogIndexState data in the future (possibly in the relcache), but currently this optimization is the caller's responsibility.

## Parameters / Member Variables
- `heapRel`: The system catalog relation containing the tuple to be updated
- `otid`: ItemPointer identifying the specific tuple to be updated
- `tup`: The new HeapTuple data that will replace the existing tuple
- `indstate`: Pre-opened CatalogIndexState containing information about all indexes on the relation

## Dependencies
- Functions called/Symbols referenced:
  - [CatalogIndexState](CatalogIndexState.md)
  - TU_UpdateIndexes
  - TU_All
  - [CatalogTupleCheckConstraints](CatalogTupleCheckConstraints.md)
  - [simple_heap_update](../s/simple_heap_update.md)
  - [CatalogIndexInsert](CatalogIndexInsert.md)
- Called from (representative examples):
  - [update_attstats](../u/update_attstats.md)
  - [swap_relation_files](../s/swap_relation_files.md)
  - [MakeConfigurationMapping](../M/MakeConfigurationMapping.md)
  - [inv_write](../i/inv_write.md)
  - [inv_truncate](../i/inv_truncate.md)

## Notes and Other Information
- This function is part of PostgreSQL's catalog management infrastructure, optimized for bulk update operations
- The caller is responsible for managing the CatalogIndexState lifecycle (opening and closing indexes)
- All catalog constraints are checked before the update to maintain system catalog integrity
- The TU_All flag ensures that all indexes are updated during the operation
- The updateIndexes variable is initialized to TU_All but can be modified by simple_heap_update if needed
- Commonly used in statistics updates, relation file swapping, and large object operations where multiple catalog modifications occur
- More efficient than CatalogTupleUpdate for multiple update operations since it avoids repeated index state management

## Simplified Source

```c
void
CatalogTupleUpdateWithInfo(Relation heapRel, ItemPointer otid, HeapTuple tup,
                          CatalogIndexState indstate)
{
    TU_UpdateIndexes updateIndexes = TU_All;

    // Check catalog constraints before updating
    CatalogTupleCheckConstraints(heapRel, tup);

    // Update the heap tuple
    simple_heap_update(heapRel, otid, tup, &updateIndexes);

    // Update all associated indexes using provided index state
    CatalogIndexInsert(indstate, tup, updateIndexes);
}
```