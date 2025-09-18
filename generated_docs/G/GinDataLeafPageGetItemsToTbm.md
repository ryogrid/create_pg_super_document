# GinDataLeafPageGetItemsToTbm

## Location
src/backend/access/gin/gindatapage.c: 182 - 210

## Overview
Extracts all TIDs from a GIN data leaf page and adds them directly to a TID bitmap for efficient bulk operations.

## Definition
```c
int GinDataLeafPageGetItemsToTbm(Page page, TIDBitmap *tbm)
```

## Detailed Description
This function reads all TIDs from a GIN data leaf page and adds them directly to a TID bitmap (tbm) without creating an intermediate array. This approach is more memory-efficient than `GinDataLeafPageGetItems` when the goal is to accumulate TIDs into a bitmap for bulk operations.

The function handles both compressed and uncompressed page formats:
- For compressed pages, it uses `ginPostingListDecodeAllSegmentsToTbm` to decode posting list segments directly into the bitmap
- For uncompressed pages, it retrieves the TID array and adds all items to the bitmap using `tbm_add_tuples`

This function is optimized for scenarios where TIDs will be used for bitmap operations rather than individual processing.

## Parameters / Member Variables
- `page`: The GIN data leaf page to extract TIDs from
- `tbm`: The TID bitmap to add the extracted TIDs to

## Dependencies
- Functions called/Symbols referenced:
  - GinPageIsCompressed
  - GinDataLeafPageGetPostingList
  - GinDataLeafPageGetPostingListSize
  - [ginPostingListDecodeAllSegmentsToTbm](../g/ginPostingListDecodeAllSegmentsToTbm.md)
  - [dataLeafPageGetUncompressed](../d/dataLeafPageGetUncompressed.md)
  - [tbm_add_tuples](../t/tbm_add_tuples.md)
- Called from (representative examples):
  - [scanPostingTree](../s/scanPostingTree.md)
  - [GinBtreeDataLeafInsertData](GinBtreeDataLeafInsertData.md)

## Notes and Other Information
- Returns the number of TIDs added to the bitmap
- More memory-efficient than `GinDataLeafPageGetItems` when TIDs are destined for bitmap operations
- The function modifies the passed TID bitmap in-place
- Handles both compressed and uncompressed page formats transparently