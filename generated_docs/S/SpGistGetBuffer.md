# SpGistGetBuffer

## Location
src/backend/access/spgist/spgutils.c: 561 - 664

## Overview
Retrieves a buffer page for SP-GiST index operations with specified type, parity, and minimum free space requirements, utilizing a cache-first strategy for efficiency.

## Definition
Buffer SpGistGetBuffer(Relation index, int flags, int needSpace, bool *isNew)

## Detailed Description
This function implements an intelligent buffer allocation strategy for SP-GiST indexes by first checking the lastUsedPages cache before allocating new pages. It validates that the requested space requirements can be satisfied (even on an empty page), then applies the index's fillfactor to the space request to maintain optimal page utilization.

The function employs a three-tier approach: first checking cache for suitable existing pages, then attempting to reuse cached pages even if they need reinitialization, and finally falling back to allocating completely new pages. When examining cached pages, it performs comprehensive validation including lock availability, page state, type compatibility, null-handling requirements, and actual free space.

The function sets the isNew output parameter to indicate whether the returned page was newly initialized, helping callers understand the page's state and optimize their subsequent operations.

## Parameters / Member Variables
- : Relation object representing the SP-GiST index requiring a buffer page
- : Bit flags specifying page type (leaf/inner), null handling, and parity requirements
- : Minimum required free space in bytes that the returned page must provide
- : Output parameter set to true if the page was initialized during this call, false if it was already valid

## Dependencies
- Functions called/Symbols referenced:
  - spgGetCache
  - SpGistGetTargetPageFreeSpace
  - GET_LUP
  - allocNewBuffer
  - SpGistBlockIsFixed
  - ReadBuffer
  - ConditionalLockBuffer
  - ReleaseBuffer
  - BufferGetPage
  - PageIsNew
  - SpGistPageIsDeleted
  - PageIsEmpty
  - SpGistInitBuffer
  - PageGetExactFreeSpace
  - SpGistPageIsLeaf
  - SpGistPageStoresNulls
  - UnlockReleaseBuffer
- Called from (representative examples):
  - moveLeafs
  - doPickSplit
  - spgAddNodeAction
  - spgSplitNodeAction
  - spgdoinsert

## Notes and Other Information
The function includes error checking to prevent requests for impossible space amounts exceeding SPGIST_PAGE_CAPACITY. It uses ConditionalLockBuffer to avoid blocking on busy pages, preferring to allocate new pages rather than wait. The fillfactor consideration helps maintain index performance by preserving space for related tuples. Cache entries are updated with actual free space measurements after successful allocations to maintain accuracy.