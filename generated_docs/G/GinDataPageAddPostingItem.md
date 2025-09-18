# GinDataPageAddPostingItem

## Location
src/backend/access/gin/gindatapage.c: 380 - 416

## Overview
GinDataPageAddPostingItem inserts a PostingItem into a non-leaf GIN data page at a specified offset, handling memory management and page layout updates.

## Definition
void GinDataPageAddPostingItem(Page page, PostingItem *data, OffsetNumber offset)

## Detailed Description
This function adds a PostingItem to a non-leaf GIN data page at the specified offset position. If the offset is InvalidOffsetNumber, the item is appended to the end. For insertions at specific positions, it shifts existing items to make space using memmove. The function also maintains the page's metadata by updating the maximum offset counter and adjusting the page data size to reflect the new layout. This ensures proper page structure for both storage efficiency and correct B-tree operations.

## Parameters / Member Variables
- page: The non-leaf data page where the PostingItem will be inserted
- data: Pointer to the PostingItem structure to be added
- offset: Target position for insertion, or InvalidOffsetNumber to append at the end

## Dependencies
- Functions called/Symbols referenced:
  - GinPageGetOpaque
  - PostingItemGetBlockNumber
  - GinPageIsLeaf
  - GinDataPageGetPostingItem
  - GinDataPageSetDataSize
  - InvalidOffsetNumber
  - memmove
  - memcpy
- Called from (representative examples):
  - dataExecPlaceToPageInternal
  - ginDataFillRoot
  - ginRedoInsertData
  - GinBtreeDataLeafInsertData

## Notes and Other Information
- Only operates on non-leaf GIN data pages (verified by assertion)
- Validates that the PostingItem contains a valid block number before insertion
- Efficiently handles both append operations and insertions at specific positions
- Maintains standard page layout by updating pd_lower to optimize full-page images
- Uses memmove for safe overlapping memory operations when shifting existing items
- Part of the GIN (Generalized Inverted Index) access method implementation
- Essential for B-tree maintenance operations including splits and redistributions