# PageIsFull

## Location
[src/include/storage/bufpage.h:411-415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/bufpage.h#L411-L415)

## Overview
Checks whether a page is marked as full by examining the PD_PAGE_FULL flag in the page header.

## Definition
static inline bool PageIsFull(Page page)

## Detailed Description
This function tests the PD_PAGE_FULL bit in the page header's pd_flags field to determine if the page has been marked as full. When this flag is set, it indicates that the page has insufficient space for new tuple insertions, serving as an optimization hint to avoid attempting insertions that would likely fail. This flag-based approach helps PostgreSQL's storage management system quickly identify pages that should be skipped during space allocation operations.

The PD_PAGE_FULL flag is a performance optimization that prevents unnecessary work by marking pages that are known to be at or near capacity. This helps reduce the overhead of repeatedly checking page space during high-volume insertion operations.

## Parameters / Member Variables
- page: A pointer to the page whose full status is being checked

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (cast to access page header structure)
  - PD_PAGE_FULL (flag constant)
- Called from (representative examples):
  - [heap_page_prune_opt](../h/heap_page_prune_opt.md) (src/backend/access/heap/pruneheap.c:242)
  - [heap_page_prune_opt](../h/heap_page_prune_opt.md) (src/backend/access/heap/pruneheap.c:253)
  - [heap_page_prune_and_freeze](../h/heap_page_prune_and_freeze.md) (src/backend/access/heap/pruneheap.c:667)

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- The function only checks the flag state and does not modify the page
- Used primarily in heap page pruning and space management operations
- The flag is managed by the PageSetFull function
- Helps optimize insertion performance by avoiding futile space allocation attempts on full pages