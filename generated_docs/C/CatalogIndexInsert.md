# CatalogIndexInsert

## Location
[src/backend/catalog/indexing.c:75-194](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/indexing.c#L75-L194)

## Overview
CatalogIndexInsert creates and inserts index entries for a single catalog tuple across all indexes of a system catalog relation, serving as a simplified version of ExecInsertIndexTuples optimized for system catalog operations.

## Definition
```c
static void CatalogIndexInsert(CatalogIndexState indstate, HeapTuple heapTuple, TU_UpdateIndexes updateIndexes)
```

## Detailed Description
CatalogIndexInsert is the core function responsible for maintaining index consistency when tuples are inserted or updated in PostgreSQL system catalogs. It iterates through all indexes associated with a catalog relation and creates appropriate index entries for a given heap tuple.

The function implements several optimizations and restrictions specific to system catalogs:
- Supports HOT (Heap-Only Tuples) optimization by skipping index insertions when appropriate
- Handles summarizing indexes separately through the updateIndexes parameter
- Enforces restrictions that system catalogs do not support expressional indexes, partial indexes, exclusion constraints, or deferred uniqueness
- Uses a temporary TupleTableSlot for tuple processing to avoid the overhead of full execution state management

The function validates index readiness, extracts appropriate column values using FormIndexDatum, and delegates the actual index insertion to index_insert with proper uniqueness checking based on the index type.

## Parameters / Member Variables
- `indstate`: CatalogIndexState containing information about opened indexes and the heap relation
- `heapTuple`: The heap tuple for which index entries should be created
- `updateIndexes`: Specifies which types of indexes to update (normal, summarizing, or both)

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleIsHeapOnly (checks if tuple is HOT)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md) (creates temporary slot)
  - [ExecStoreHeapTuple](../E/ExecStoreHeapTuple.md) (stores tuple in slot)
  - [FormIndexDatum](../F/FormIndexDatum.md) (extracts index column values)
  - [index_insert](../i/index_insert.md) (performs actual index insertion)
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md) (cleanup temporary slot)
  - [ReindexIsProcessingIndex](../R/ReindexIsProcessingIndex.md) (assertion checking during reindex)
- Called from (representative examples):
  - [CatalogTupleInsert](CatalogTupleInsert.md)
  - [CatalogTupleInsertWithInfo](CatalogTupleInsertWithInfo.md)
  - [CatalogTuplesMultiInsertWithInfo](CatalogTuplesMultiInsertWithInfo.md)
  - [CatalogTupleUpdate](CatalogTupleUpdate.md)
  - [CatalogTupleUpdateWithInfo](CatalogTupleUpdateWithInfo.md)

## Notes and Other Information
- This is a static function, only used internally within the indexing.c module
- Implements HOT optimization by skipping index updates for heap-only tuples when appropriate
- Enforces that system catalogs only use simple indexes without expressions, predicates, or exclusion constraints
- Supports selective index updating through the TU_UpdateIndexes parameter for operations that only need to update summarizing indexes
- Uses assertion checks to validate that system catalog restrictions are maintained
- Properly handles unique vs non-unique indexes by setting appropriate uniqueness check flags
- Creates and destroys a temporary TupleTableSlot for each operation to avoid execution state overhead

## Simplified Source

```c
// Simplified version of CatalogIndexInsert
static void CatalogIndexInsert(CatalogIndexState indstate, HeapTuple heapTuple,
                               TU_UpdateIndexes updateIndexes) {
    int numIndexes;
    RelationPtr relationDescs;
    TupleTableSlot *slot;
    IndexInfo **indexInfoArray;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    bool onlySummarized = (updateIndexes == TU_Summarizing);

    // HOT optimization: skip index updates for heap-only tuples
#ifndef USE_ASSERT_CHECKING
    if (HeapTupleIsHeapOnly(heapTuple) && !onlySummarized)
        return;
#endif

    // Get state information and check if there are indexes to update
    numIndexes = indstate->ri_NumIndices;
    if (numIndexes == 0)
        return;

    relationDescs = indstate->ri_IndexRelationDescs;
    indexInfoArray = indstate->ri_IndexRelationInfo;

    // Create temporary slot for tuple processing
    slot = MakeSingleTupleTableSlot(RelationGetDescr(indstate->ri_RelationDesc),
                                    &TTSOpsHeapTuple);
    ExecStoreHeapTuple(heapTuple, slot, false);

    // Process each index
    for (int i = 0; i < numIndexes; i++) {
        IndexInfo *indexInfo = indexInfoArray[i];
        Relation index = relationDescs[i];

        // Skip if index not ready or only updating summarizing indexes
        if (!indexInfo->ii_ReadyForInserts)
            continue;
        if (onlySummarized && !indexInfo->ii_Summarizing)
            continue;

        // Extract index column values from tuple
        FormIndexDatum(indexInfo, slot, NULL, values, isnull);

        // Insert into index with appropriate uniqueness checking
        index_insert(index, values, isnull, &(heapTuple->t_self),
                    indstate->ri_RelationDesc,
                    index->rd_index->indisunique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO,
                    false, indexInfo);
    }

    ExecDropSingleTupleTableSlot(slot);
}
```

Key simplifications made:
- Core logic: create slot, iterate indexes, extract values, insert index entries
- HOT optimization skips unnecessary index updates for heap-only tuples
- System catalog restrictions ensure only simple indexes without expressions
- Selective updating supports summarizing vs normal indexes separately