# tbm_get_pageentry

## Location
[src/backend/nodes/tidbitmap.c:1202-1248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L1202-L1248)

## Overview
Finds or creates a PagetableEntry for a specified page number in a TID bitmap, managing the transition between different bitmap states as needed.

## Definition

```c
static PagetableEntry *
tbm_get_pageentry(TIDBitmap *tbm, BlockNumber pageno)
```
## Detailed Description
This function is a core internal utility for TID bitmap management that handles finding an existing PagetableEntry or creating a new one for a given page number. It manages the dynamic transitions between different bitmap states:

1. **Empty state (TBM_EMPTY)**: Uses the fixed slot (entry1) and transitions to TBM_ONE_PAGE
2. **Single page state (TBM_ONE_PAGE)**: Returns the existing entry if it matches the requested page, otherwise creates a hash table and transitions to multi-page mode
3. **Multi-page state**: Uses hash table lookup/insertion via pagetable_insert

When creating a new entry, the function initializes it as an exact (non-chunk) entry and updates the bitmap's entry and page counters. The function may cause the bitmap to exceed desired memory limits, requiring the caller to invoke tbm_lossify() when appropriate.

## Parameters / Member Variables
- `*tbm`: Pointer to the TIDBitmap structure containing the page table
- `pageno`: Block number of the page for which to find or create an entry
## Dependencies
- Functions called/Symbols referenced:
  - [tbm_create_pagetable](tbm_create_pagetable.md) (to transition from single-page to hash table mode)
  - pagetable_insert (to insert entries in hash table mode)
  - MemSet (to initialize new page entries)
- Types used:
  - [TIDBitmap](../T/TIDBitmap.md)
  - [PagetableEntry](../P/PagetableEntry.md)
  - BlockNumber
- Constants used:
  - TBM_EMPTY, TBM_ONE_PAGE (bitmap status flags)
- Called from:
  - [tbm_add_tuples](tbm_add_tuples.md)
  - [tbm_union_page](tbm_union_page.md)
  - Referenced in TBMSharedIterator

## Notes and Other Information
- This is a static function, only accessible within tidbitmap.c
- The function handles automatic state transitions in the bitmap structure
- May cause memory usage to exceed limits - caller responsibility to call tbm_lossify()
- New entries are always marked as exact (non-chunk) initially
- The function preserves the status field when reinitializing existing page entries