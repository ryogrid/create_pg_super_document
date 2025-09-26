# tbm_union

## Location
[src/backend/nodes/tidbitmap.c:458-480](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L458-L480)

## Overview
Performs a set union operation between two TIDBitmaps, merging all pages from the second bitmap into the first.

## Definition
```c
void tbm_union(TIDBitmap *a, const TIDBitmap *b)
```

## Detailed Description
The `tbm_union` function implements a set union operation for TIDBitmaps. It modifies the first bitmap (a) in-place by adding all pages present in the second bitmap (b). The second bitmap remains unchanged. This operation is fundamental for combining results from multiple index scans or bitmap operations.

The function handles different internal representations of TIDBitmaps. If the source bitmap contains only a single page (TBM_ONE_PAGE status), it directly calls `tbm_union_page` on that page. For bitmaps with multiple pages (TBM_HASH status), it iterates through all pages in the hash table and unions each one individually.

## Parameters / Member Variables
- `a`: Target TIDBitmap that will be modified to include all pages from both bitmaps
- `b`: Source TIDBitmap whose pages will be merged into the target (remains unchanged)

## Dependencies
- Functions called/Symbols referenced:
  - [tbm_union_page](tbm_union_page.md)
  - pagetable_start_iterate  
  - pagetable_iterate
  - TBM_ONE_PAGE (constant)
  - TBM_HASH (constant)
  - [PagetableEntry](../P/PagetableEntry.md) (type)
- Called from (representative examples):
  - [MultiExecBitmapOr](../M/MultiExecBitmapOr.md) (in src/backend/executor/nodeBitmapOr.c:170)

## Notes and Other Information
- The function asserts that the target bitmap is not currently being iterated over
- Returns early if the source bitmap is empty (nentries == 0)
- Used primarily in bitmap OR operations during query execution
- The union operation preserves both exact and lossy page representations
- Memory management and potential lossification are handled by the underlying `tbm_union_page` calls