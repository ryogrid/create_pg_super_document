# index_delete_sort_cmp

## Location
src/backend/access/heap/heapam.c: 8404 - 8439

## Overview
A specialized inlineable comparison function used for sorting TM_IndexDelete structures by their ItemPointer (TID) values in index_delete_sort().

## Definition
```c
static inline int index_delete_sort_cmp(TM_IndexDelete *deltid1, TM_IndexDelete *deltid2)
```

## Detailed Description
This function provides a comparison routine for sorting TM_IndexDelete structures based on their tuple identifiers (TIDs). It implements a two-level comparison strategy: first by block number, then by offset number within the block. This ordering ensures that deletion operations are performed in physical storage order, which can improve I/O efficiency when processing multiple index deletions.

The function uses an inlineable design to minimize function call overhead during sorting operations. It includes an assertion that should never be reached, indicating that the function expects all compared TIDs to be distinct.

## Parameters / Member Variables
- `deltid1`: Pointer to the first TM_IndexDelete structure to compare
- `deltid2`: Pointer to the second TM_IndexDelete structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - ItemPointerGetBlockNumber
  - ItemPointerGetOffsetNumber
  - TM_IndexDelete (structure type)
- Called from (representative examples):
  - index_delete_sort

## Notes and Other Information
- The function is declared as static inline for performance optimization
- Returns -1 if deltid1 < deltid2, 1 if deltid1 > deltid2, or 0 if equal (though the Assert(false) suggests equality should not occur)
- The comparison is performed hierarchically: block number takes precedence over offset number
- Located in src/backend/access/heap/heapam.c:8404-8439