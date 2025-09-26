# tbm_intersect

## Location
src/backend/nodes/tidbitmap.c: 540 - 588

## Overview
Performs a set intersection operation between two TIDBitmaps, modifying the first bitmap to contain only pages present in both bitmaps.

## Definition
```c
void tbm_intersect(TIDBitmap *a, const TIDBitmap *b)
```

## Detailed Description
The `tbm_intersect` function implements a set intersection operation for TIDBitmaps. It modifies the first bitmap (a) in-place by removing all pages that are not present in the second bitmap (b). The second bitmap remains unchanged. This operation is fundamental for combining results from multiple index scans using AND logic.

The function handles different internal representations of TIDBitmaps. For single-page bitmaps (TBM_ONE_PAGE status), it calls `tbm_intersect_page` directly on the single entry and may transition the bitmap to TBM_EMPTY status if the page becomes empty. For multi-page bitmaps (TBM_HASH status), it iterates through all pages in the hash table, intersecting each one with the second bitmap and removing empty pages from the hash table.

The function maintains proper entry counts and status transitions, ensuring the bitmap structure remains consistent after the operation.

## Parameters / Member Variables
- `a`: Target TIDBitmap that will be modified to contain the intersection
- `b`: Source TIDBitmap used for comparison (remains unchanged)

## Dependencies
- Functions called/Symbols referenced:
  - tbm_intersect_page
  - pagetable_start_iterate
  - pagetable_iterate  
  - pagetable_delete
  - elog
  - TBM_ONE_PAGE (constant)
  - TBM_HASH (constant)
  - TBM_EMPTY (constant)
  - PagetableEntry (type)
- Called from (representative examples):
  - MultiExecBitmapAnd (in src/backend/executor/nodeBitmapAnd.c:144)

## Notes and Other Information
- The function asserts that the target bitmap is not currently being iterated over
- Returns early if the target bitmap is empty (no intersection possible)
- Properly handles removal of empty pages and chunks from the hash table
- Used primarily in bitmap AND operations during query execution
- Maintains accurate counts of pages, chunks, and total entries
- May transition bitmap status from TBM_ONE_PAGE to TBM_EMPTY if the single page becomes empty
- Error handling includes corruption detection for hash table operations