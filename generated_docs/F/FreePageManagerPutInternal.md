# FreePageManagerPutInternal

## Location
[src/backend/utils/mmgr/freepage.c:1476-1842](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L1476-L1842)

## Overview
The core deallocation function that returns a range of pages to the Free Page Manager, handling consolidation with adjacent spans and B-tree management including splits and reorganization.

## Definition
```c
static Size FreePageManagerPutInternal(FreePageManager *fpm, Size first_page, Size npages, bool soft)
```

## Detailed Description
This complex function handles returning freed pages to the Free Page Manager with sophisticated consolidation and B-tree management. It operates in several modes: singleton mode (before B-tree initialization) where it manages a single free span and handles consolidation by extending existing spans or initializing the B-tree when non-contiguous. In B-tree mode, it searches for insertion points, consolidates with adjacent entries (both preceding and following), and performs B-tree operations including splits when necessary. The function can operate in 'soft' mode where it avoids allocating new B-tree pages, useful for cleanup operations. It maintains freelist integrity throughout all operations and returns the size of the final consolidated span.

## Parameters / Member Variables
- `fpm`: Pointer to the FreePageManager instance
- `first_page`: Starting page number of the span being returned
- `npages`: Number of contiguous pages being returned
- `soft`: If true, avoid operations that would require allocating new B-tree pages

## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base
  - fpm_page_to_pointer
  - FreePagePushSpanLeader
  - [FreePagePopSpanLeader](FreePagePopSpanLeader.md)
  - FreePageBtreeGetRecycled
  - [FreePageManagerGetInternal](FreePageManagerGetInternal.md)
  - FreePageBtreeSearch
  - FreePageBtreeFindRightSibling
  - FreePageBtreeRemove
  - [FreePageBtreeAdjustAncestorKeys](FreePageBtreeAdjustAncestorKeys.md)
  - FreePageBtreeRecycle
  - FreePageBtreeSplitPage
  - FreePageBtreeInsertLeaf
  - FreePageBtreeInsertInternal
  - FreePageBtreeSearchLeaf
  - FreePageBtreeSearchInternal
  - FreePageBtreeFirstKey
  - relptr_store
  - relptr_access
- Called from (representative examples):
  - [FreePageManagerPut](FreePageManagerPut.md)
  - [FreePageBtreeCleanup](FreePageBtreeCleanup.md)

## Notes and Other Information
- Returns 0 if soft flag prevented insertion, otherwise returns size of consolidated span
- Handles both singleton mode (before B-tree initialization) and full B-tree mode
- Performs intelligent consolidation with adjacent free spans to reduce fragmentation
- Manages B-tree splits including root splits that increase tree depth
- Uses recycled B-tree pages when available to minimize allocation overhead
- Critical function for PostgreSQL's memory deallocation and defragmentation
- Can trigger complex multi-level B-tree restructuring operations
- Maintains consistency between B-tree structure and freelists throughout all operations