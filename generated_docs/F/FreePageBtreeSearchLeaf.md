# FreePageBtreeSearchLeaf

## Location
[src/backend/utils/mmgr/freepage.c:1170-1200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L1170-L1200)

## Overview
Performs binary search on a leaf btree page to find the first key greater than or equal to a given page number.

## Definition
```c
static Size FreePageBtreeSearchLeaf(FreePageBtree *btp, Size first_page)
```

## Detailed Description
This function implements binary search specifically for leaf pages in the FreePageBtree structure. Unlike internal page searches, leaf page searches operate on the actual data entries that contain the complete information about free page ranges. The function locates either an exact match for the target page number or determines the correct insertion position to maintain the sorted order of leaf keys.

The search algorithm is identical to the internal page version but operates on leaf-specific data structures and uses leaf page magic number validation. This specialization allows for type-safe operations and proper handling of leaf-specific data layouts.

## Parameters / Member Variables
- `btp`: Pointer to the FreePageBtree leaf page structure to search within
- `first_page`: The target page number to locate in the leaf page keys

## Dependencies
- Functions called/Symbols referenced:
  - FREE_PAGE_LEAF_MAGIC (magic number validation)
  - FPM_ITEMS_PER_LEAF_PAGE (page capacity constant)
- Called from:
  - FreePageBtreeRemovePage
  - FreePageBtreeSearch
  - FreePageManagerPutInternal

## Notes and Other Information
- Implements the same O(log n) binary search algorithm as the internal page version
- Uses leaf-specific magic number (FREE_PAGE_LEAF_MAGIC) for page type validation
- Returns index that may exceed nused when target is larger than all existing keys
- Essential for precise positioning within leaf pages during insertions, deletions, and lookups
- The returned index directly corresponds to positions in the leaf_key array for data manipulation operations