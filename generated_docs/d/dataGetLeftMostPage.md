# dataGetLeftMostPage

## Location
[src/backend/access/gin/gindatapage.c:364-379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L364-L379)

## Overview
dataGetLeftMostPage retrieves the block number of the leftmost child page from a non-leaf GIN data page.

## Definition
static BlockNumber dataGetLeftMostPage(GinBtree btree, Page page)

## Detailed Description
This function extracts the block number of the leftmost child page from a GIN B-tree internal (non-leaf) data page. It accesses the first PostingItem on the page and returns its associated block number. This operation is fundamental for B-tree traversal, particularly when navigating to the leftmost path during range scans or when finding the minimum key position in a subtree.

## Parameters / Member Variables
- btree: GinBtree structure containing B-tree context information  
- page: The non-leaf data page from which to extract the leftmost child pointer

## Dependencies
- Functions called/Symbols referenced:
  - GinPageIsLeaf
  - GinPageIsData
  - GinPageGetOpaque
  - GinDataPageGetPostingItem
  - PostingItemGetBlockNumber
  - FirstOffsetNumber
- Called from (representative examples):
  - [ginPrepareDataScan](../g/ginPrepareDataScan.md)

## Notes and Other Information
- Only operates on non-leaf GIN data pages (verified by assertions)
- Assumes the page has at least one PostingItem (checked by maxoff >= FirstOffsetNumber assertion)
- Simple and efficient operation that directly accesses the first item without searching
- Essential for B-tree navigation patterns that require accessing the leftmost subtree
- Part of the GIN (Generalized Inverted Index) access method implementation

## Simplified Source

```c
static BlockNumber dataGetLeftMostPage(GinBtree btree, Page page) {
    // Get the first PostingItem on the page
    PostingItem *pitem = GinDataPageGetPostingItem(page, FirstOffsetNumber);

    // Return the block number it points to
    return PostingItemGetBlockNumber(pitem);
}
```