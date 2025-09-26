# FreePageBtreeSearchInternal

## Location
src/backend/utils/mmgr/freepage.c: 1140 - 1169

## Overview
Performs binary search on an internal btree page to find the first key greater than or equal to a given page number.

## Definition
```c
static Size FreePageBtreeSearchInternal(FreePageBtree *btp, Size first_page)
```

## Detailed Description
This function implements a standard binary search algorithm specifically designed for internal btree pages. It searches through the keys stored in an internal page to find the appropriate position for a given page number. The search returns either the index of an exact match or the position where the key would be inserted to maintain the sorted order of keys.

The function is optimized for btree traversal operations where precise positioning within internal nodes is critical for maintaining btree invariants. It uses the classic divide-and-conquer approach with low and high pointers to efficiently narrow down the search space.

## Parameters / Member Variables
- `btp`: Pointer to the FreePageBtree internal page structure to search within
- `first_page`: The target page number to locate in the internal page keys

## Dependencies
- Functions called/Symbols referenced:
  - FREE_PAGE_INTERNAL_MAGIC (magic number validation)
  - FPM_ITEMS_PER_INTERNAL_PAGE (page capacity constant)
- Called from:
  - FreePageBtreeAdjustAncestorKeys
  - FreePageBtreeFindLeftSibling
  - FreePageBtreeFindRightSibling
  - FreePageBtreeRemovePage
  - FreePageBtreeSearch
  - FreePageManagerPutInternal

## Notes and Other Information
- Uses binary search algorithm for O(log n) time complexity
- Returns index that may be equal to nused if the target is larger than all existing keys
- Includes assertion checks to verify the page is actually an internal page type
- Critical for btree navigation and maintaining proper key ordering during insertions and deletions
- The returned index can be used directly for child page selection in btree descent operations