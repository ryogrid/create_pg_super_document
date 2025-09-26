# FreePageBtreeInsertInternal

## Location
[src/backend/utils/mmgr/freepage.c:900-916](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L900-L916)

## Overview
Inserts an item into an internal page of a free page btree structure, maintaining the tree ordering by shifting existing entries.

## Definition
```c
static void FreePageBtreeInsertInternal(char *base, FreePageBtree *btp, Size index,
                                       Size first_page, FreePageBtree *child)
```

## Detailed Description
This function performs insertion of a new key-child pair into an internal node of the free page btree at the specified index position. It uses `memmove` to shift existing entries to the right to make space for the new entry, then stores the first page number and child pointer at the insertion point. The function maintains the btree invariants by preserving the ordering of keys and ensuring proper parent-child relationships.

## Parameters / Member Variables
- `base`: Base address of the memory manager segment used for relative pointer calculations
- `btp`: Pointer to the internal btree page where the insertion will occur
- `index`: Zero-based position where the new entry should be inserted (must be ≤ nused)
- `first_page`: The first page number that will serve as the key for this entry
- `child`: Pointer to the child btree node that this entry will reference

## Dependencies
- Functions called/Symbols referenced:
  - memmove
  - relptr_store
  - FREE_PAGE_INTERNAL_MAGIC
  - FPM_ITEMS_PER_INTERNAL_PAGE
  - [FreePageBtreeInternalKey](FreePageBtreeInternalKey.md)
- Called from (representative examples):
  - [FreePageManagerPutInternal](FreePageManagerPutInternal.md)

## Notes and Other Information
- Function includes assertions to validate that the target page has the correct internal magic number
- Ensures the page is not full (nused ≤ FPM_ITEMS_PER_INTERNAL_PAGE)
- Validates that the insertion index is within valid bounds
- Uses relative pointers (relptr_store) for child references to support shared memory segments
- Increments the nused counter to reflect the new entry