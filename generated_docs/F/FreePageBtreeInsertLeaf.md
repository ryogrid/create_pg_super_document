# FreePageBtreeInsertLeaf

## Location
[src/backend/utils/mmgr/freepage.c:917-933](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L917-L933)

## Overview
Inserts a free page span entry into a leaf page of a free page btree, maintaining sorted order by shifting existing entries.

## Definition
```c
static void FreePageBtreeInsertLeaf(FreePageBtree *btp, Size index, Size first_page,
                                   Size npages)
```

## Detailed Description
This function performs insertion of a new free page span into a leaf node of the free page btree at the specified index position. It shifts existing leaf entries to the right using `memmove` to create space for the new entry, then stores the first page number and page count. The function maintains the btree leaf structure and ensures proper ordering of free page spans.

## Parameters / Member Variables
- `btp`: Pointer to the leaf btree page where the insertion will occur
- `index`: Zero-based position where the new entry should be inserted (must be ≤ nused)
- `first_page`: The starting page number of the free page span
- `npages`: The number of consecutive pages in this free span

## Dependencies
- Functions called/Symbols referenced:
  - memmove
  - FREE_PAGE_LEAF_MAGIC
  - FPM_ITEMS_PER_LEAF_PAGE
  - [FreePageBtreeLeafKey](FreePageBtreeLeafKey.md)
- Called from (representative examples):
  - [FreePageManagerPutInternal](FreePageManagerPutInternal.md)

## Notes and Other Information
- Function includes assertions to validate that the target page has the correct leaf magic number
- Ensures the leaf page is not full (nused ≤ FPM_ITEMS_PER_LEAF_PAGE)
- Validates that the insertion index is within valid bounds
- Leaf entries store actual free page span information (first_page, npages) rather than pointers
- Increments the nused counter to reflect the new entry
- Does not require base address parameter since leaf nodes store data directly, not relative pointers