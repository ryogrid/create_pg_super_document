# PageGetHeapFreeSpace

## Location
[src/backend/storage/page/bufpage.c:991-1051](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/page/bufpage.c#L991-L1051)

## Overview
Returns the size of the free (allocatable) space on a heap page, reduced by the space needed for a new line pointer, while enforcing heap-specific line pointer limits.

## Definition

```c
Size
PageGetHeapFreeSpace(Page page)
```
## Detailed Description
PageGetHeapFreeSpace is the heap-specific variant of PageGetFreeSpace that includes additional checks to enforce PostgreSQL's MaxHeapTuplesPerPage limit. While it starts by calling PageGetFreeSpace to get the basic free space calculation, it then performs heap-specific validation to ensure that no more than MaxHeapTuplesPerPage line pointers exist on the page.

The function implements a two-tier check: first, it verifies if the maximum number of line pointers has been reached, and if so, it checks whether any existing line pointers are free (unused). If the page has reached the maximum line pointer limit and no free line pointers are available, it returns 0 regardless of the actual free space. This prevents the creation of too many line pointers, which could break assumptions in other parts of the codebase.

The function also handles hint validation - it checks if the PageHasFreeLinePointers hint is accurate by scanning through line pointers when necessary, though it cannot correct incorrect hints due to lack of permission to mark the page dirty.

## Parameters / Member Variables
- : A pointer to the heap page for which to calculate free space

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetFreeSpace](PageGetFreeSpace.md)
  - [PageGetMaxOffsetNumber](PageGetMaxOffsetNumber.md)
  - MaxHeapTuplesPerPage
  - [PageHasFreeLinePointers](PageHasFreeLinePointers.md)
  - FirstOffsetNumber
  - OffsetNumberNext
  - [PageGetItemId](PageGetItemId.md)
  - ItemId
  - ItemIdIsUsed
- Called from (representative examples):
  - [heap_multi_insert](../h/heap_multi_insert.md)
  - [heap_update](../h/heap_update.md)
  - [heap_xlog_prune_freeze](../h/heap_xlog_prune_freeze.md)
  - [heap_xlog_insert](../h/heap_xlog_insert.md)
  - [heap_xlog_multi_insert](../h/heap_xlog_multi_insert.md)
  - [heap_xlog_update](../h/heap_xlog_update.md)
  - [RelationGetBufferForTuple](../R/RelationGetBufferForTuple.md)
  - [heap_page_prune_opt](../h/heap_page_prune_opt.md)
  - [raw_heap_insert](../r/raw_heap_insert.md)
  - [lazy_scan_heap](../l/lazy_scan_heap.md)
  - [lazy_vacuum_heap_rel](../l/lazy_vacuum_heap_rel.md)

## Notes and Other Information
- Specifically designed for heap pages, unlike PageGetFreeSpace which is primarily for index pages
- Enforces MaxHeapTuplesPerPage limit to prevent excessive line pointer creation
- Includes hint validation for PageHasFreeLinePointers but cannot correct incorrect hints
- Returns 0 when line pointer limit is reached and no free line pointers are available
- Prevents breaking assumptions in code that relies on MaxHeapTuplesPerPage as a hard limit
- Handles both redirected and dead line pointers in its calculations
- Located in src/backend/storage/page/bufpage.c:991-1051

## Simplified Source
```c
Size
PageGetHeapFreeSpace(Page page)
{
    Size space = PageGetFreeSpace(page);

    if (space > 0) {
        OffsetNumber nline = PageGetMaxOffsetNumber(page);

        // Check if we've reached the maximum line pointer limit
        if (nline >= MaxHeapTuplesPerPage) {
            if (PageHasFreeLinePointers(page)) {
                // Verify there's actually a free line pointer
                bool found_free = false;
                for (OffsetNumber offnum = FirstOffsetNumber;
                     offnum <= nline;
                     offnum = OffsetNumberNext(offnum)) {
                    ItemId lp = PageGetItemId(page, offnum);
                    if (!ItemIdIsUsed(lp)) {
                        found_free = true;
                        break;
                    }
                }

                if (!found_free) {
                    // Hint was wrong - no space available
                    space = 0;
                }
            } else {
                // No free line pointers available
                space = 0;
            }
        }
    }

    return space;
}
```