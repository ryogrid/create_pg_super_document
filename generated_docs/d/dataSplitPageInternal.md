# dataSplitPageInternal

## Location
[src/backend/access/gin/gindatapage.c:1252-1332](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L1252-L1332)

## Overview
dataSplitPageInternal splits an internal GIN data page when there is insufficient space for a new PostingItem, creating two new temporary pages with the existing and new items distributed between them.

## Definition
```c
static void dataSplitPageInternal(GinBtree btree, Buffer origbuf,
                                 GinBtreeStack *stack,
                                 void *insertdata, BlockNumber updateblkno,
                                 Page *newlpage, Page *newrpage)
```

## Detailed Description
This function handles the complex process of splitting an internal GIN data page when a new PostingItem cannot fit. It creates two temporary pages, constructs a merged array of all PostingItems (existing plus new), determines an optimal split point, and distributes the items between the left and right pages. The function also handles updating the downlink pointer of the existing item at the insertion offset and properly sets up the right boundary keys for both resulting pages.

The split strategy varies based on whether this is an index build operation on a rightmost page, where items are packed as tightly as possible on the left page for optimal space utilization during sequential scans.

## Parameters / Member Variables
- `btree`: GIN B-tree structure containing tree metadata and build status information
- `origbuf`: Buffer containing the original internal data page to be split
- `stack`: GIN B-tree stack indicating the insertion position and offset
- `insertdata`: Pointer to the new PostingItem to be inserted during the split
- `updateblkno`: Block number to update the existing item's downlink pointer to
- `newlpage`: Output parameter for the resulting left page
- `newrpage`: Output parameter for the resulting right page

## Dependencies
- Functions called/Symbols referenced:
  - GinPageGetOpaque
  - [PageGetPageSize](../P/PageGetPageSize.md)
  - GinDataPageGetRightBound
  - [PageGetTempPage](../P/PageGetTempPage.md)
  - [GinInitPage](../G/GinInitPage.md)
  - GinDataPageGetPostingItem
  - PostingItemSetBlockNumber
  - GinPageRightMost
  - GinNonLeafDataPageGetFreeSpace
  - GinDataPageSetDataSize
  - [PostingItem](../P/PostingItem.md) (struct)
  - FirstOffsetNumber (constant)
- Called from:
  - [dataBeginPlaceToPageInternal](dataBeginPlaceToPageInternal.md)
  - leafSegmentInfo

## Notes and Other Information
- Creates temporary pages using PageGetTempPage that must be managed by the caller
- Uses a local array allitems to construct the complete merged item list before distribution
- Implements different split strategies for index build vs. normal operations
- During index builds on rightmost pages, maximizes left page utilization for better packing
- Properly maintains right boundary keys for both resulting pages to preserve B-tree ordering
- Updates both maxoff counters and pd_lower values to maintain page consistency
- The original buffer remains untouched, with all work done on temporary page copies
- Handles the downlink update as part of the split process using PostingItemSetBlockNumber