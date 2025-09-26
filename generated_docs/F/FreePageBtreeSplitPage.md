# FreePageBtreeSplitPage

## Location
src/backend/utils/mmgr/freepage.c: 1201 - 1231

## Overview
Allocates a new btree page and moves half the keys from an existing full page to the new page to maintain btree balance.

## Definition
```c
static FreePageBtree *FreePageBtreeSplitPage(FreePageManager *fpm, FreePageBtree *btp)
```

## Detailed Description
This function implements the core page splitting logic required when btree pages become full during insertion operations. It creates a new sibling page and redistributes keys between the original page and the new page, typically moving half the keys to maintain balanced tree structure. The function handles both leaf and internal page types, with special handling for internal pages that requires updating parent pointers of moved child pages.

The splitting operation is essential for maintaining btree properties while accommodating new data. The caller is responsible for ensuring that a recycled page is available and for establishing the proper parent-child relationships after the split.

## Parameters / Member Variables
- `fpm`: Pointer to the FreePageManager containing the btree structure and recycled page pool
- `btp`: Pointer to the full FreePageBtree page that needs to be split

## Dependencies
- Functions called/Symbols referenced:
  - FreePageBtreeGetRecycled
  - relptr_copy
  - FreePageBtreeUpdateParentPointers
  - fpm_segment_base
  - FREE_PAGE_LEAF_MAGIC (magic number constants)
  - FREE_PAGE_INTERNAL_MAGIC
  - FreePageBtreeLeafKey (data structure types)
  - FreePageBtreeInternalKey
- Called from:
  - FreePageManagerPutInternal

## Notes and Other Information
- Splits pages roughly in half to maintain balanced tree structure
- Uses memcpy for efficient key movement between pages
- Handles both leaf and internal page types with appropriate data structure awareness
- For internal pages, calls FreePageBtreeUpdateParentPointers to maintain parent-child consistency
- Returns the new sibling page that caller must integrate into the btree structure
- Relies on the caller to have ensured recycled page availability before invocation
- Critical component of btree growth and rebalancing during high insertion load