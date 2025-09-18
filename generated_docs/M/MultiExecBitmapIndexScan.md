# MultiExecBitmapIndexScan

## Location
src/backend/executor/nodeBitmapIndexscan.c: 49 - 130

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
  - InstrStartNode (for instrumentation support)
  - ExecReScan (for runtime key setup)
  - tbm_create (to create new TID bitmap)
  - index_getbitmap (to collect TIDs from index)
  - ExecIndexAdvanceArrayKeys (for array key iteration)
  - index_rescan (to reset index scan for array keys)
  - InstrStopNode (for instrumentation support)
- Called from (representative examples):
  - MultiExecProcNode (from the executor framework)

## Notes and Other Information
- This is the primary execution function for bitmap index scans, handling the bulk TID collection
- Supports both regular scan keys and array keys for IN-clause expressions
- Can merge results into pre-existing bitmaps for UNION operations
- Uses work_mem to size the TID bitmap appropriately
- Provides instrumentation support for query performance monitoring
- Handles interrupts during long-running scans via CHECK_FOR_INTERRUPTS()
- Located at src/backend/executor/nodeBitmapIndexscan.c:49-130