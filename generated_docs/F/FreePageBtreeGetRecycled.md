# FreePageBtreeGetRecycled

## Location
[src/backend/utils/mmgr/freepage.c:880-899](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L880-L899)

## Overview
Retrieves a recycled page from the B-tree recycle list for reuse as a new B-tree page, implementing efficient page recycling within the free page manager.

## Definition
static FreePageBtree *FreePageBtreeGetRecycled(FreePageManager *fpm)

## Detailed Description
This function manages the B-tree page recycling mechanism by extracting a page from the btree_recycle linked list. When B-tree pages are no longer needed (e.g., after consolidation or removal operations), they are placed on a recycle list rather than being immediately returned to the general free page pool. This allows for efficient reuse of pages that are already formatted for B-tree operations.

The function performs the following operations:
1. Accesses the head of the recycle list via fpm->btree_recycle
2. Updates the recycle list to point to the next available page
3. Properly updates the doubly-linked list pointers
4. Decrements the recycle count
5. Returns the recycled page cast as a FreePageBtree pointer

The function assumes the recycle list is not empty (asserted) and ensures proper page alignment before returning the recycled page.

## Parameters / Member Variables
- : Pointer to the FreePageManager containing the recycle list

## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base
  - relptr_access
  - relptr_copy
  - relptr_store
  - fpm_pointer_is_page_aligned
- Called from (representative examples):
  - FreePageBtreeCleanup
  - FreePageBtreeSplitPage
  - FreePageManagerPutInternal

## Notes and Other Information
- Assumes the recycle list contains at least one page (asserted with victim != NULL)
- Properly manages the doubly-linked list structure of the recycle list
- Decrements btree_recycle_count to maintain accurate count tracking
- Ensures page alignment before returning the recycled page
- The recycled page is reused as-is without additional formatting, improving performance
- Part of the broader page lifecycle management system that optimizes memory usage in the free page manager