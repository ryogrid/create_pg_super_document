# index_getnext_slot

## Location
src/backend/access/index/indexam.c: 673 - 717

## Overview
The `index_getnext_slot` function provides a high-level interface for retrieving complete tuples from an index scan, combining TID retrieval and heap tuple fetching into a single operation.

## Definition
```c
bool index_getnext_slot(IndexScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
```

## Detailed Description
This function serves as a comprehensive tuple retrieval interface that orchestrates both index scanning and heap tuple fetching. It implements the complete scan logic:

1. Checks if heap continuation is needed (for HOT chains) or if a new TID should be fetched
2. Calls `index_getnext_tid` to get the next matching index entry when needed
3. Calls `index_fetch_heap` to retrieve the actual heap tuple corresponding to the TID
4. Loops until either a visible tuple is found or the scan is exhausted
5. Handles HOT chain traversal by respecting the `xs_heap_continue` flag

The function encapsulates the typical index-to-heap scanning pattern, making it easier for callers to iterate through matching tuples without managing the TID-to-tuple conversion manually.

## Parameters / Member Variables
- `scan`: IndexScanDesc structure representing the active index scan
- `direction`: ScanDirection indicating forward or backward scan direction  
- `slot`: TupleTableSlot where the retrieved tuple will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [index_getnext_tid](index_getnext_tid.md) (retrieves next TID from index)
  - [index_fetch_heap](index_fetch_heap.md) (fetches heap tuple from TID)
  - [ItemPointerEquals](../I/ItemPointerEquals.md) (validates TID consistency)
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md) (validates TID)
  - [IndexScanDesc](../I/IndexScanDesc.md) (scan descriptor type)
  - `ScanDirection` (direction enumeration)
- Called from (representative examples):
  - [IndexNext](../I/IndexNext.md) (src/backend/executor/nodeIndexscan.c:130)
  - [systable_getnext](../s/systable_getnext.md) (src/backend/access/index/genam.c:511)
  - [check_exclusion_or_unique_constraint](../c/check_exclusion_or_unique_constraint.md) (src/backend/executor/execIndexing.c:780)

## Notes and Other Information
- The caller must check `scan->xs_recheck` and perform scan key rechecking if required
- Resources like buffer pins are automatically managed and will be cleaned up in subsequent calls
- The function handles HOT chain continuation automatically through the `xs_heap_continue` flag
- Returns false when no more matching tuples exist, indicating end of scan
- This is the most commonly used interface for index scanning when full tuples are needed
- Location: src/backend/access/index/indexam.c:673-717