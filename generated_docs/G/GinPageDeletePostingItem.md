# GinPageDeletePostingItem

## Location
[src/backend/access/gin/gindatapage.c:417-447](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L417-L447)

## Overview
GinPageDeletePostingItem removes a PostingItem from a non-leaf GIN data page at a specified offset and compacts the remaining items.

## Definition
void GinPageDeletePostingItem(Page page, OffsetNumber offset)

## Detailed Description
This function removes a PostingItem from a non-leaf GIN data page at the specified offset position. When the deleted item is not the last one on the page, it shifts all subsequent items forward using memmove to eliminate the gap. The function also updates the page metadata by decrementing the maximum offset counter and adjusting the page data size to reflect the reduced number of items. This maintains proper page structure and ensures efficient space utilization.

## Parameters / Member Variables
- page: The non-leaf data page from which to delete the PostingItem
- offset: The offset position of the PostingItem to be removed

## Dependencies
- Functions called/Symbols referenced:
  - GinPageGetOpaque
  - GinPageIsLeaf  
  - GinDataPageGetPostingItem
  - GinDataPageSetDataSize
  - FirstOffsetNumber
  - memmove
- Called from (representative examples):
  - [ginDeletePage](../g/ginDeletePage.md)
  - [ginRedoDeletePage](../g/ginRedoDeletePage.md)
  - [GinBtreeDataLeafInsertData](GinBtreeDataLeafInsertData.md)

## Notes and Other Information
- Only operates on non-leaf GIN data pages (verified by assertion)
- Validates that the offset is within valid bounds before deletion
- Efficiently handles deletion from any position using memmove for safe memory operations
- Updates page layout metadata to maintain consistency with the standard page format
- Does not perform deletion from the last position optimization (uses memmove regardless)
- Part of the GIN (Generalized Inverted Index) access method implementation
- Essential for B-tree maintenance operations including page merging and vacuuming