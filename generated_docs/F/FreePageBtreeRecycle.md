# FreePageBtreeRecycle

## Location
src/backend/utils/mmgr/freepage.c: 934 - 954

## Overview
Adds a single page to the btree recycle list for later reuse, maintaining a doubly-linked list of recycled btree pages.

## Definition
```c
static void FreePageBtreeRecycle(FreePageManager *fpm, Size pageno)
```

## Detailed Description
This function adds a page to the btree recycle list by converting it into a FreePageSpanLeader structure and inserting it at the head of the doubly-linked recycle list. The recycled pages can be later reused for new btree nodes, improving memory efficiency. The function properly initializes the span leader structure with appropriate magic numbers and maintains the linked list integrity by updating prev/next pointers.

## Parameters / Member Variables
- `fpm`: Pointer to the FreePageManager structure that manages the free page system
- `pageno`: The page number to be added to the recycle list

## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base
  - relptr_access
  - fpm_page_to_pointer
  - relptr_store
  - FREE_PAGE_SPAN_LEADER_MAGIC
  - FreePageSpanLeader
- Called from (representative examples):
  - FreePageBtreeCleanup
  - FreePageBtreeRemovePage
  - FreePageManagerPutInternal

## Notes and Other Information
- Initializes the recycled page as a span leader with magic number for validation
- Sets npages to 1 since individual btree pages are being recycled
- Maintains doubly-linked list structure by properly setting prev/next pointers
- Inserts new recycled pages at the head of the recycle list
- Increments btree_recycle_count to track the number of recycled pages
- Uses relative pointers for shared memory compatibility
- Handles the case where the recycle list was previously empty (head == NULL)