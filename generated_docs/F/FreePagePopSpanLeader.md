# FreePagePopSpanLeader

## Location
src/backend/utils/mmgr/freepage.c: 1843 - 1870

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
  - FreePageBtreeCleanup
  - FreePageManagerPutInternal (multiple calls during consolidation operations)

## Notes and Other Information
- This is a static utility function used internally by the Free Page Manager
- Essential for maintaining freelist integrity during span consolidation and allocation
- Handles both middle-of-list removal and head-of-list removal cases
- Uses relative pointers for memory management in shared memory contexts
- Critical for proper memory management during span size changes and allocation operations
- Assumes the span leader exists at the given page number and contains valid list pointers