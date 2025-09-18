# CatalogIndexInsert

## Location
src/backend/catalog/indexing.c: 75 - 194

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
  - MakeSingleTupleTableSlot (creates temporary slot)
  - ExecStoreHeapTuple (stores tuple in slot)
  - FormIndexDatum (extracts index column values)
  - index_insert (performs actual index insertion)
  - ExecDropSingleTupleTableSlot (cleanup temporary slot)
  - ReindexIsProcessingIndex (assertion checking during reindex)
- Called from (representative examples):
  - CatalogTupleInsert
  - CatalogTupleInsertWithInfo
  - CatalogTuplesMultiInsertWithInfo
  - CatalogTupleUpdate
  - CatalogTupleUpdateWithInfo

## Notes and Other Information
- This is a static function, only used internally within the indexing.c module
- Implements HOT optimization by skipping index updates for heap-only tuples when appropriate
- Enforces that system catalogs only use simple indexes without expressions, predicates, or exclusion constraints
- Supports selective index updating through the TU_UpdateIndexes parameter for operations that only need to update summarizing indexes
- Uses assertion checks to validate that system catalog restrictions are maintained
- Properly handles unique vs non-unique indexes by setting appropriate uniqueness check flags
- Creates and destroys a temporary TupleTableSlot for each operation to avoid execution state overhead