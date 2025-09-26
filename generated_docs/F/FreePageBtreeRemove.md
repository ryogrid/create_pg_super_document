# FreePageBtreeRemove

## Location
src/backend/utils/mmgr/freepage.c: 955 - 986

## Overview
Removes an item from a btree leaf page at a specified index, handling page removal, key adjustments, and consolidation as needed.

## Definition
```c
static void FreePageBtreeRemove(FreePageManager *fpm, FreePageBtree *btp, Size index)
```

## Detailed Description
This function removes an entry from a btree leaf page and maintains btree integrity through several operations. If the page becomes empty (last item removed), it removes the entire page from the btree. For partial removals, it physically removes the key using `memmove` to shift remaining entries. When the first key is removed, it adjusts ancestor keys to maintain proper btree ordering. Finally, it considers consolidating the page with siblings if the page becomes too sparse.

## Parameters / Member Variables
- `fpm`: Pointer to the FreePageManager structure managing the free page system
- `btp`: Pointer to the btree leaf page from which to remove the item
- `index`: Zero-based index of the item to remove (must be < nused)

## Dependencies
- Functions called/Symbols referenced:
  - FreePageBtreeRemovePage
  - memmove
  - FreePageBtreeAdjustAncestorKeys
  - FreePageBtreeConsolidate
  - FREE_PAGE_LEAF_MAGIC
  - FreePageBtreeLeafKey
- Called from (representative examples):
  - FreePageManagerGetInternal
  - FreePageManagerPutInternal

## Notes and Other Information
- Function includes assertions to validate leaf page magic number and index bounds
- Handles the special case where removing the last item requires page removal
- Uses memmove to efficiently shift array elements after removal
- Only calls ancestor key adjustment when the first (index 0) key is removed
- Always attempts consolidation after removal to maintain btree balance
- Decrements nused counter before physical key removal to maintain consistency
- Consolidation may merge pages or redistribute keys with siblings