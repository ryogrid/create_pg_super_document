# reorderqueue_pop

## Location
[src/backend/executor/nodeIndexscan.c:492-518](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexscan.c#L492-L518)

## Overview
A helper function that removes and returns the next tuple from the reorder queue in PostgreSQL's index scan operations, handling proper cleanup of associated ordering data.

## Definition
```c
static HeapTuple reorderqueue_pop(IndexScanState *node)
```

## Detailed Description
This function retrieves the topmost tuple from a pairing heap-based reorder queue used during K-nearest neighbor (KNN) index scans. It removes the first ReorderTuple from the pairing heap, extracts the HeapTuple, and performs proper memory cleanup of the associated ordering values and metadata structures.

The function is responsible for memory management of the ReorderTuple structure and its components, including freeing any pass-by-reference ordering values that were copied during the push operation. It carefully checks the iss_OrderByTypByVals array to determine which ordering values require explicit cleanup.

## Parameters / Member Variables
- `node`: IndexScanState containing the reorder queue and scan configuration information

## Return Value
- Returns a HeapTuple that was stored in the reorder queue, now ready for processing

## Dependencies
- Functions called/Symbols referenced:
  - pairingheap_remove_first
  - [pfree](../p/pfree.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - ReorderTuple (struct)
  - [IndexScanState](../I/IndexScanState.md) (struct)
- Called from (representative examples):
  - [IndexNextWithReorder](../I/IndexNextWithReorder.md)
  - [ExecReScanIndexScan](../E/ExecReScanIndexScan.md)

## Notes and Other Information
- Part of PostgreSQL's KNN index scan implementation for ordered result retrieval
- Performs selective cleanup of ordering values based on their storage characteristics (by-value vs by-reference)
- The function is static, indicating it's only used within the nodeIndexscan.c file
- Properly handles memory management to prevent memory leaks during query execution
- Works in conjunction with reorderqueue_push to maintain an ordered queue of tuples