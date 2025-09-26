# FreePageManagerGetInternal

## Location
[src/backend/utils/mmgr/freepage.c:1319-1475](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L1319-L1475)

## Overview
The core allocation function that finds and allocates a contiguous run of pages from the Free Page Manager using a best-fit strategy across multiple freelists.

## Definition
```c
static bool FreePageManagerGetInternal(FreePageManager *fpm, Size npages, Size *first_page)
```

## Detailed Description
This function implements the main page allocation logic for the Free Page Manager. It searches through freelists using a best-fit policy, starting with the appropriately-sized list and moving to larger lists as needed. For fixed-size lists, it takes the first available span, but for the oversized list (last freelist), it searches for the smallest span that satisfies the request. After finding a suitable span, it removes it from the freelist, updates the B-tree structure if necessary, handles partial allocation by splitting spans, and maintains the contiguous_pages tracking. The function handles both the initial singleton span case and the full B-tree managed case.

## Parameters / Member Variables
- `fpm`: Pointer to the FreePageManager instance
- `npages`: Number of contiguous pages to allocate
- `first_page`: Output parameter receiving the starting page number of the allocated span

## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base
  - fpm_pointer_to_page  
  - relptr_access
  - relptr_copy
  - relptr_is_null
  - [FreePagePushSpanLeader](FreePagePushSpanLeader.md)
  - [FreePageBtreeSearch](FreePageBtreeSearch.md)
  - [FreePageBtreeRemove](FreePageBtreeRemove.md)
  - [FreePageBtreeAdjustAncestorKeys](FreePageBtreeAdjustAncestorKeys.md)
- Called from (representative examples):
  - [FreePageManagerGet](FreePageManagerGet.md)
  - [FreePageManagerPutInternal](FreePageManagerPutInternal.md) (internal use for span consolidation)

## Notes and Other Information
- Uses best-fit allocation strategy which may cause fragmentation but is suitable for PostgreSQL's typical allocation patterns
- Handles both singleton span mode (before B-tree initialization) and full B-tree mode
- Updates contiguous_pages tracking when removing spans that might affect the largest available span
- Splits spans when allocated size is smaller than found span
- Returns false if no suitable span is available
- Maintains freelist integrity by properly unlinking spans and updating prev/next pointers
- Critical function for PostgreSQL's memory management subsystem performance