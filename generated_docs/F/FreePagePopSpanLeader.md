# FreePagePopSpanLeader

## Location
[src/backend/utils/mmgr/freepage.c:1843-1870](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L1843-L1870)

## Overview
A utility function that removes a FreePageSpanLeader from its doubly-linked freelist, updating the list pointers and freelist head as necessary.

## Definition
```c
static void FreePagePopSpanLeader(FreePageManager *fpm, Size pageno)
```

## Detailed Description
This function removes a FreePageSpanLeader from the doubly-linked list freelist that contains it. The function locates the span leader at the given page number, extracts its next and previous pointers, and updates the adjacent nodes' pointers to maintain list integrity. When removing the head of a freelist (when prev is NULL), it updates the corresponding freelist array entry to point to the next span. The function handles all necessary pointer management using relative pointer operations to maintain the linked list structure.

## Parameters / Member Variables
- `fpm`: Pointer to the FreePageManager instance containing the freelists
- `pageno`: Page number where the FreePageSpanLeader to be removed is located

## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base
  - fpm_page_to_pointer
  - relptr_access
  - relptr_copy
  - FPM_NUM_FREELISTS
  - FPM_PAGE_SIZE
- Called from (representative examples):
  - [FreePageBtreeCleanup](FreePageBtreeCleanup.md)
  - [FreePageManagerPutInternal](FreePageManagerPutInternal.md) (multiple calls during consolidation operations)

## Notes and Other Information
- This is a static utility function used internally by the Free Page Manager
- Essential for maintaining freelist integrity during span consolidation and allocation
- Handles both middle-of-list removal and head-of-list removal cases
- Uses relative pointers for memory management in shared memory contexts
- Critical for proper memory management during span size changes and allocation operations
- Assumes the span leader exists at the given page number and contains valid list pointers

## Simplified Source

```c
// Simplified version of FreePagePopSpanLeader
static void FreePagePopSpanLeader(FreePageManager *fpm, Size pageno) {
    char *base = fmp_segment_base(fpm);
    FreePageSpanLeader *span = (FreePageSpanLeader *) fpm_page_to_pointer(base, pageno);

    // Get adjacent nodes
    FreePageSpanLeader *next = relptr_access(base, span->next);
    FreePageSpanLeader *prev = relptr_access(base, span->prev);

    // Update linked list pointers
    if (next != NULL)
        relptr_copy(next->prev, span->prev);
    if (prev != NULL) {
        relptr_copy(prev->next, span->next);
    } else {
        // Removing head of freelist - update freelist array
        Size f = Min(span->npages, FPM_NUM_FREELISTS) - 1;
        Assert(relptr_offset(fpm->freelist[f]) == pageno * FPM_PAGE_SIZE);
        relptr_copy(fpm->freelist[f], span->next);
    }
}
```

Key simplifications made:
- Preserved essential linked list removal logic
- Maintained special case handling for list head removal
- Kept freelist array update for head removal
- Focused on core pointer manipulation algorithm