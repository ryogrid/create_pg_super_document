# reorderqueue_push

## Location
src/backend/executor/nodeIndexscan.c: 458 - 491

## Overview
A helper function that adds a tuple to the reorder queue in PostgreSQL's index scan operations, creating a deep copy of the tuple and its ordering values for later retrieval in sorted order.

## Definition
```c
static void reorderqueue_push(IndexScanState *node, TupleTableSlot *slot, Datum *orderbyvals, bool *orderbynulls)
```

## Detailed Description
This function is responsible for adding tuples to a pairing heap-based reorder queue used during K-nearest neighbor (KNN) index scans. It creates a ReorderTuple structure containing a deep copy of the input tuple and its associated ordering values. The function operates within the query memory context to ensure proper memory management throughout the query execution.

The function allocates memory for the ReorderTuple and copies the heap tuple from the slot, along with the ordering values used for sorting. For non-null ordering values, it performs a deep copy using datumCopy to ensure data integrity. The completed ReorderTuple is then added to the pairing heap for later ordered retrieval.

## Parameters / Member Variables
- `node`: IndexScanState containing the reorder queue and scan configuration
- `slot`: TupleTableSlot containing the tuple to be added to the queue
- `orderbyvals`: Array of Datum values used for ordering the tuple
- `orderbynulls`: Array of boolean flags indicating which ordering values are NULL

## Dependencies
- Functions called/Symbols referenced:
  - ExecCopySlotHeapTuple
  - [palloc](../p/palloc.md)
  - [datumCopy](../d/datumCopy.md)
  - pairingheap_add
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
- Called from (representative examples):
  - [IndexNextWithReorder](../I/IndexNextWithReorder.md)

## Notes and Other Information
- Operates within the query memory context (es_query_cxt) for proper memory management
- Creates deep copies of both tuple data and ordering values to prevent data corruption
- Part of PostgreSQL's KNN index scan implementation for ordered result retrieval
- The function is static, indicating it's only used within the nodeIndexscan.c file
- Handles NULL ordering values by setting them to (Datum) 0 while preserving the null flag