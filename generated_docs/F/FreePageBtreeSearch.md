# FreePageBtreeSearch

## Location
[src/backend/utils/mmgr/freepage.c:1064-1139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L1064-L1139)

## Overview
Searches the btree for an entry with the given first page and initializes the search result structure with position information for exact matches or insertion points.

## Definition
```c
static void FreePageBtreeSearch(FreePageManager *fpm, Size first_page, FreePageBtreeSearchResult *result)
```

## Detailed Description
This function performs a comprehensive search through the FreePageBtree structure to locate a specific page entry or determine where a new entry should be inserted. The search process begins at the root and descends through internal nodes until reaching a leaf page. During traversal, it also calculates the number of additional btree pages that would be needed for potential splits during insertion operations.

The function handles both exact matches and insertion scenarios. For exact matches, it returns the precise location of the entry. For insertion cases, it identifies the correct position where the new key should be placed while maintaining the btree ordering properties.

## Parameters / Member Variables
- `fpm`: Pointer to the FreePageManager structure containing the btree root and segment information
- `first_page`: The target page number to search for in the btree
- `result`: Output parameter that will contain the search results including page location, index, match status, and split requirements

## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base
  - relptr_access
  - FreePageBtreeSearchInternal
  - FreePageBtreeSearchLeaf
- Called from:
  - FreePageManagerGetInternal
  - FreePageManagerPutInternal

## Notes and Other Information
- The function calculates split_pages to indicate how many additional btree pages would be needed for insertion
- Uses magic numbers (FREE_PAGE_INTERNAL_MAGIC) to distinguish between internal and leaf pages during traversal
- Implements btree descent logic by choosing appropriate child nodes based on key comparisons
- Maintains btree invariants by ensuring proper parent-child relationships during traversal
- The search result structure provides complete information needed for both lookup and insertion operations