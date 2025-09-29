# ExecReScanIndexScan

## Location
[src/backend/executor/nodeIndexscan.c:551-598](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexscan.c#L551-L598)

## Overview
Restarts an index scan operation with potentially new runtime-calculated scan keys, handling cleanup of reorder queues and reinitializing the index scan descriptor.

## Definition
```c
void ExecReScanIndexScan(IndexScanState *node)
```

## Detailed Description
ExecReScanIndexScan is responsible for restarting index scan operations in PostgreSQL's executor. The function handles three main tasks: recalculating runtime keys if they exist, flushing any existing reorder queue, and reinitializing the underlying index scan. This functionality is crucial for nested loop joins and other operations that require repeatedly scanning the same index with different parameter values.

The function first evaluates any runtime keys - these are scan key values that depend on variables not known at plan time, such as values from outer tables in joins. It then cleans up any existing reorder queue by popping and freeing all remaining tuples. Finally, it calls index_rescan to restart the underlying index scan with the updated keys and resets various state flags to indicate the scan is ready to begin again.

## Parameters / Member Variables
- `node`: IndexScanState containing the scan state, runtime keys, reorder queue, and scan descriptor to be reset

## Dependencies
- Functions called/Symbols referenced:
  - ResetExprContext
  - [ExecIndexEvalRuntimeKeys](ExecIndexEvalRuntimeKeys.md)
  - pairingheap_is_empty
  - [reorderqueue_pop](../r/reorderqueue_pop.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [index_rescan](../i/index_rescan.md)
  - [ExecScanReScan](ExecScanReScan.md)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (generic plan node rescan dispatcher)

## Notes and Other Information
- Part of PostgreSQL's executor node framework for plan execution
- Integrates runtime key calculation functionality that was formerly handled in ExecUpdateIndexScanKeys
- Essential for nested loop joins where the inner index scan is repeatedly executed with different outer tuple values
- Properly handles memory management by resetting expression contexts and freeing reorder queue tuples
- Uses the previously processed heap_freetuple and index_rescan functions for cleanup and reinitialization
- The iss_RuntimeKeysReady flag ensures runtime keys are properly prepared before the next scan begins
- Works with both standard index scans and KNN scans that use reorder queues

## Simplified Source

```c
void ExecReScanIndexScan(IndexScanState *node) {
    // Step 1: Recalculate runtime keys if present
    if (node->iss_NumRuntimeKeys != 0) {
        ExprContext *econtext = node->iss_RuntimeContext;

        // Reset context to prevent memory leaks
        ResetExprContext(econtext);

        // Evaluate all runtime key expressions
        ExecIndexEvalRuntimeKeys(econtext,
                                node->iss_RuntimeKeys,
                                node->iss_NumRuntimeKeys);
    }
    node->iss_RuntimeKeysReady = true;

    // Step 2: Clean up reorder queue if it exists
    if (node->iss_ReorderQueue) {
        HeapTuple tuple;

        while (!pairingheap_is_empty(node->iss_ReorderQueue)) {
            tuple = reorderqueue_pop(node);
            heap_freetuple(tuple);
        }
    }

    // Step 3: Restart the index scan with updated keys
    if (node->iss_ScanDesc) {
        index_rescan(node->iss_ScanDesc,
                    node->iss_ScanKeys, node->iss_NumScanKeys,
                    node->iss_OrderByKeys, node->iss_NumOrderByKeys);
    }
    node->iss_ReachedEnd = false;

    // Step 4: Complete the scan framework rescan
    ExecScanReScan(&node->ss);
}
```