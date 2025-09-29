# FreePageBtreeUpdateParentPointers

## Location
[src/backend/utils/mmgr/freepage.c:1232-1249](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L1232-L1249)

## Overview
Updates the parent pointers of all child pages when internal pages are split or merged to maintain btree structural integrity.

## Definition
```c
static void FreePageBtreeUpdateParentPointers(char *base, FreePageBtree *btp)
```

## Detailed Description
This function ensures btree structural consistency by updating parent pointers in child pages after internal page modifications. When internal pages undergo split or merge operations, the children that were moved to new parent locations must have their parent pointers updated to reflect the new parent-child relationships. This maintains the bidirectional linking that allows efficient btree traversal in both directions.

The function iterates through all child pointers stored in the internal page and updates each child's parent pointer to reference the current internal page. This is essential for maintaining btree invariants and enabling proper navigation during subsequent operations.

## Parameters / Member Variables
- `base`: Pointer to the base address of the shared memory segment
- `btp`: Pointer to the internal btree page whose children need parent pointer updates

## Dependencies
- Functions called/Symbols referenced:
  - relptr_access (to access child pages via relative pointers)
  - relptr_store (to update parent pointers in child pages)
  - FREE_PAGE_INTERNAL_MAGIC (magic number for internal pages)
- Called from (representative examples):
  - [FreePageBtreeConsolidate](FreePageBtreeConsolidate.md)
  - [FreePageBtreeSplit](FreePageBtreeSplit.md)

## Notes and Other Information
- Only operates on internal pages (asserted via magic number check)
- Updates all children referenced by the internal page's key entries
- Critical for maintaining btree structural integrity after page modifications
- Uses relative pointer operations for shared memory compatibility

## Simplified Source

```c
static void FreePageBtreeUpdateParentPointers(char *base, FreePageBtree *btp)
{
    Size i;

    Assert(btp->hdr.magic == FREE_PAGE_INTERNAL_MAGIC);
    for (i = 0; i < btp->hdr.nused; ++i)
    {
        FreePageBtree *child;

        child = relptr_access(base, btp->u.internal_key[i].child);
        relptr_store(base, child->hdr.parent, btp);
    }
}
```