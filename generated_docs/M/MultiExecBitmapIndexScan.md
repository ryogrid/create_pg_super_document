# MultiExecBitmapIndexScan

## Location
[src/backend/executor/nodeBitmapIndexscan.c:49-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeBitmapIndexscan.c#L49-L130)

## Overview
MultiExecBitmapIndexScan performs the actual work of a bitmap index scan operation, collecting all tuple identifiers (TIDs) that satisfy the scan conditions and returning them as a bitmap for efficient heap access.

## Definition
```c
Node *MultiExecBitmapIndexScan(BitmapIndexScanState *node)
```

## Detailed Description
This function is the core execution routine for bitmap index scan nodes. Unlike traditional index scans that return tuples one at a time, bitmap index scans collect all qualifying TIDs at once and return them as a bitmap data structure. This approach is particularly efficient for queries that need to access many scattered tuples from a heap, as it allows the subsequent bitmap heap scan to access heap pages in physical order.

The function handles runtime key evaluation, manages array keys for IN-clause expressions, and can either create a new bitmap or merge results into an existing bitmap provided by a parent node (useful for UNION operations). It uses the index access method's amgetbitmap function to efficiently collect all qualifying TIDs in a single operation.

## Parameters / Member Variables
- `node`: Pointer to BitmapIndexScanState containing the execution state for this bitmap index scan, including scan descriptor, runtime keys, and result bitmap

## Dependencies
- Functions called/Symbols referenced:
  - [InstrStartNode](../I/InstrStartNode.md) (for instrumentation support)
  - [ExecReScan](../E/ExecReScan.md) (for runtime key setup)
  - [tbm_create](../t/tbm_create.md) (to create new TID bitmap)
  - [index_getbitmap](../i/index_getbitmap.md) (to collect TIDs from index)
  - [ExecIndexAdvanceArrayKeys](../E/ExecIndexAdvanceArrayKeys.md) (for array key iteration)
  - [index_rescan](../i/index_rescan.md) (to reset index scan for array keys)
  - [InstrStopNode](../I/InstrStopNode.md) (for instrumentation support)
- Called from (representative examples):
  - [MultiExecProcNode](MultiExecProcNode.md) (from the executor framework)

## Notes and Other Information
- This is the primary execution function for bitmap index scans, handling the bulk TID collection
- Supports both regular scan keys and array keys for IN-clause expressions
- Can merge results into pre-existing bitmaps for UNION operations
- Uses work_mem to size the TID bitmap appropriately
- Provides instrumentation support for query performance monitoring
- Handles interrupts during long-running scans via CHECK_FOR_INTERRUPTS()
- Located at src/backend/executor/nodeBitmapIndexscan.c:49-130

## Simplified Source

```c
Node *
MultiExecBitmapIndexScan(BitmapIndexScanState *node)
{
    TIDBitmap *tbm;
    IndexScanDesc scandesc;
    double nTuples = 0;
    bool doscan;

    // Start performance instrumentation
    if (node->ss.ps.instrument)
        InstrStartNode(node->ss.ps.instrument);

    scandesc = node->biss_ScanDesc;

    // Handle runtime keys and array keys
    if (!node->biss_RuntimeKeysReady &&
        (node->biss_NumRuntimeKeys != 0 || node->biss_NumArrayKeys != 0))
    {
        ExecReScan((PlanState *) node);
        doscan = node->biss_RuntimeKeysReady;
    }
    else
        doscan = true;

    // Prepare result bitmap - use existing or create new
    if (node->biss_result)
    {
        tbm = node->biss_result;
        node->biss_result = NULL;  // Reset for next time
    }
    else
    {
        // Create new bitmap using work_mem
        tbm = tbm_create(work_mem * 1024L,
                        ((BitmapIndexScan *) node->ss.ps.plan)->isshared ?
                        node->ss.ps.state->es_query_dsa : NULL);
    }

    // Scan index and collect TIDs into bitmap
    while (doscan)
    {
        nTuples += (double) index_getbitmap(scandesc, tbm);

        CHECK_FOR_INTERRUPTS();

        // Advance to next array key combination if any
        doscan = ExecIndexAdvanceArrayKeys(node->biss_ArrayKeys,
                                          node->biss_NumArrayKeys);
        if (doscan)
        {
            // Reset index scan for next array key combination
            index_rescan(node->biss_ScanDesc,
                        node->biss_ScanKeys, node->biss_NumScanKeys,
                        NULL, 0);
        }
    }

    // Stop instrumentation
    if (node->ss.ps.instrument)
        InstrStopNode(node->ss.ps.instrument, nTuples);

    return (Node *) tbm;
}
```