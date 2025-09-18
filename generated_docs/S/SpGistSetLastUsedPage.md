# SpGistSetLastUsedPage

## Location
[src/backend/access/spgist/spgutils.c:665-699](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgutils.c#L665-L699)

## Overview
Updates the lastUsedPages cache when a page has been modified, maintaining an optimal cache of pages with available free space for future insertions.

## Definition
```c
void SpGistSetLastUsedPage(Relation index, Buffer buffer)
```

## Detailed Description
This function maintains PostgreSQL's SP-GiST index performance by caching information about pages that have been recently modified and their available free space. When a page is modified during an insert, split, or vacuum operation, this function updates the cache entry to reflect the current free space available on that page. The cache helps the index access method quickly locate pages with sufficient free space for new insertions, avoiding the need to scan multiple pages.

The function categorizes pages by type (leaf vs inner) and special properties (null-storing pages), maintaining separate cache entries for each category. Fixed pages (like root pages) are never cached as they have special handling requirements.

## Parameters / Member Variables
- `index`: The SP-GiST relation being updated
- `buffer`: The buffer containing the modified page to be cached

## Dependencies
- Functions called/Symbols referenced:
  - [spgGetCache](../s/spgGetCache.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - SpGistBlockIsFixed
  - SpGistPageIsLeaf
  - SpGistPageStoresNulls
  - [PageGetExactFreeSpace](../P/PageGetExactFreeSpace.md)
  - GET_LUP (macro)
- Called from (representative examples):
  - [moveLeafs](../m/moveLeafs.md)
  - [doPickSplit](../d/doPickSplit.md)
  - [spgMatchNodeAction](../s/spgMatchNodeAction.md)
  - [spgAddNodeAction](../s/spgAddNodeAction.md)
  - [spgSplitNodeAction](../s/spgSplitNodeAction.md)
  - [spgdoinsert](../s/spgdoinsert.md)
  - [spgvacuumpage](../s/spgvacuumpage.md)
  - [spgprocesspending](../s/spgprocesspending.md)

## Notes and Other Information
- Fixed pages (root pages) are never cached as they require special handling
- The cache is organized by page type flags (leaf/inner, null-storing)
- Updates occur when the cached page is the same as the current page, when no page is cached, or when the current page has more free space than the cached page
- This optimization is crucial for SP-GiST insert performance as it avoids repeated page scans to find space for new tuples