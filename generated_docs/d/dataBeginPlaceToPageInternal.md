# dataBeginPlaceToPageInternal

## Location
[src/backend/access/gin/gindatapage.c:1119-1144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L1119-L1144)

## Overview
dataBeginPlaceToPageInternal prepares to insert data on an internal GIN data page, determining whether the insertion fits or requires a page split.

## Definition


## Detailed Description
This function is responsible for preparing an insertion operation on an internal (non-leaf) GIN data page. It evaluates whether the new PostingItem will fit on the current page by checking the available free space. If the item fits, it returns GPTP_INSERT to proceed with the insertion. If there isn't enough space, it triggers a page split operation by calling dataSplitPageInternal and returns GPTP_SPLIT.

The function is designed to be called before entering the insertion critical section and does not modify the given page buffer itself. For internal node insertions, it handles both the insertion of the new item and the update of the downlink pointer of the existing item at the specified stack offset to point to updateblkno.

## Parameters / Member Variables
- : GIN B-tree structure containing tree metadata and configuration
- : Buffer containing the target internal data page for insertion
- : GIN B-tree stack indicating the insertion position and path
- : Pointer to the data item to be inserted
- : Block number to update the downlink pointer to
- : Output parameter for passing workspace information to the execution phase
- : Output parameter for the left page in case of split
- : Output parameter for the right page in case of split

## Dependencies
- Functions called/Symbols referenced:
  - GinNonLeafDataPageGetFreeSpace
  - [dataSplitPageInternal](dataSplitPageInternal.md)
  - [PostingItem](../P/PostingItem.md) (struct)
  - GPTP_SPLIT (enum value)
  - GPTP_INSERT (enum value)
- Called from:
  - [dataBeginPlaceToPage](dataBeginPlaceToPage.md)

## Notes and Other Information
- This function is static and only used internally within the GIN data page module
- The function performs no actual page modification, serving only as a preparation step
- The free space check uses sizeof(PostingItem) as the minimum space requirement
- In split scenarios, the function delegates the actual split logic to dataSplitPageInternal
- The updateblkno parameter is specific to internal node operations where downlinks need updating