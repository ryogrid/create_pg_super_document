# FreePageBtreeRemovePage

## Location
[src/backend/utils/mmgr/freepage.c:987-1063](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L987-L1063)

## Overview
Removes an entire page from the btree structure, handling parent updates, root removal, and recycling the page for future use.

## Definition
```c
static void FreePageBtreeRemovePage(FreePageManager *fpm, FreePageBtree *btp)
```

## Detailed Description
This function removes a complete page from the btree by first traversing up the tree to find pages with only single entries (which also need removal), then removing the downlink from the parent page. It handles the special case of root removal by setting the root to NULL and depth to 0. The function searches for the appropriate entry in the parent (whether leaf or internal), removes it using memmove, and then recycles the page. After removal, it adjusts ancestor keys if the first entry was removed and considers consolidating the parent with siblings.

## Parameters / Member Variables
- `fpm`: Pointer to the FreePageManager structure managing the free page system
- `btp`: Pointer to the btree page to be removed from the tree

## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base
  - relptr_access
  - relptr_store
  - FreePageBtreeRecycle
  - fmp_pointer_to_page
  - FreePageBtreeFirstKey
  - FreePageBtreeSearchLeaf
  - FreePageBtreeSearchInternal
  - memmove
  - FreePageBtreeAdjustAncestorKeys
  - FreePageBtreeConsolidate
  - FREE_PAGE_LEAF_MAGIC
  - FreePageBtreeLeafKey
  - FreePageBtreeInternalKey
- Called from (representative examples):
  - FreePageBtreeConsolidate
  - FreePageBtreeRemove

## Notes and Other Information
- Uses a loop to recursively remove parent pages that become empty (nused == 1)
- Special handling for root page removal: sets btree_root to NULL and depth to 0
- Differentiates between leaf and internal parent pages when removing downlinks
- Uses appropriate search functions (SearchLeaf vs SearchInternal) based on parent type
- Decrements parent nused counter after removing the entry
- Always recycles removed pages to the btree recycle list for reuse
- Performs ancestor key adjustment only when the first entry (index 0) is removed
- Triggers consolidation of the parent page to maintain btree balance
- Assumes caller has already relocated any important keys from the page being removed