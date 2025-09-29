# FreePageBtreeRemove

## Location
[src/backend/utils/mmgr/freepage.c:955-986](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L955-L986)

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
  - [FreePageBtreeRemovePage](FreePageBtreeRemovePage.md)
  - memmove
  - [FreePageBtreeAdjustAncestorKeys](FreePageBtreeAdjustAncestorKeys.md)
  - [FreePageBtreeConsolidate](FreePageBtreeConsolidate.md)
  - FREE_PAGE_LEAF_MAGIC
  - [FreePageBtreeLeafKey](FreePageBtreeLeafKey.md)
- Called from (representative examples):
  - [FreePageManagerGetInternal](FreePageManagerGetInternal.md)
  - [FreePageManagerPutInternal](FreePageManagerPutInternal.md)

## Notes and Other Information
- Function includes assertions to validate leaf page magic number and index bounds
- Handles the special case where removing the last item requires page removal
- Uses memmove to efficiently shift array elements after removal
- Only calls ancestor key adjustment when the first (index 0) key is removed
- Always attempts consolidation after removal to maintain btree balance
- Decrements nused counter before physical key removal to maintain consistency
- Consolidation may merge pages or redistribute keys with siblings

## Simplified Source

```c
static void
FreePageBtreeRemove(FreePageManager *fpm, FreePageBtree *btp, Size index)
{
    Assert(btp->hdr.magic == FREE_PAGE_LEAF_MAGIC);
    Assert(index < btp->hdr.nused);

    // If removing last item, remove entire page
    if (btp->hdr.nused == 1)
    {
        FreePageBtreeRemovePage(fpm, btp);
        return;
    }

    // Remove the key by shifting remaining keys left
    --btp->hdr.nused;
    if (index < btp->hdr.nused)
        memmove(&btp->u.leaf_key[index], &btp->u.leaf_key[index + 1],
                sizeof(FreePageBtreeLeafKey) * (btp->hdr.nused - index));

    // If first key removed, update ancestor keys
    if (index == 0)
        FreePageBtreeAdjustAncestorKeys(fpm, btp);

    // Try to consolidate with siblings
    FreePageBtreeConsolidate(fpm, btp);
}
```