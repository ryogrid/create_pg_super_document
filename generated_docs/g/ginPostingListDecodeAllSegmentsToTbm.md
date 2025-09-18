# ginPostingListDecodeAllSegmentsToTbm

## Location
[src/backend/access/gin/ginpostinglist.c:358-377](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginpostinglist.c#L358-L377)

## Overview
A convenience function that decodes compressed GIN posting list segments and directly adds the resulting item pointers to a TID bitmap for efficient set operations.

## Definition


## Detailed Description
This function serves as an optimized pathway for decoding GIN posting lists when the end goal is to populate a TID bitmap. Rather than requiring the caller to manage the intermediate ItemPointer array, it handles the complete workflow: decode the posting list segments, add all items to the bitmap, and clean up the temporary memory.

The function leverages  to perform the actual decoding work, then uses  to efficiently batch-add all decoded item pointers to the target bitmap. This approach minimizes memory management overhead and provides a cleaner interface for bitmap-based operations.

This is particularly useful during query processing where multiple posting lists need to be combined into a single result set represented as a TID bitmap.

## Parameters / Member Variables
- : Pointer to the first posting list segment to decode
- : Total number of bytes containing all segments to process  
- : Target TID bitmap to receive the decoded item pointers

## Dependencies
- Functions called/Symbols referenced:
  - [ginPostingListDecodeAllSegments](ginPostingListDecodeAllSegments.md)
  - [tbm_add_tuples](../t/tbm_add_tuples.md)
  - [pfree](../p/pfree.md)
  - [GinPostingList](../G/GinPostingList.md) (type)
  - [TIDBitmap](../T/TIDBitmap.md) (type)
- Called from (representative examples):
  - [GinDataLeafPageGetItemsToTbm](../G/GinDataLeafPageGetItemsToTbm.md) (gin/gindatapage.c:192)

## Notes and Other Information
- Provides a memory-efficient interface for bitmap operations by handling temporary array allocation and cleanup internally
- Returns the number of items that were decoded and added to the bitmap
- The  parameter to  indicates that the items are already sorted (guaranteed by the posting list format)
- Commonly used during GIN index scans where results from multiple posting lists need to be combined into a single bitmap
- Eliminates the need for callers to manage the intermediate ItemPointer array lifecycle