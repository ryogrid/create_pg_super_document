# ginPostingListDecodeAllSegments

## Location
[src/backend/access/gin/ginpostinglist.c:297-357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginpostinglist.c#L297-L357)

## Overview
Decodes multiple compressed GIN posting list segments into a single array of item pointers, handling variable-length segment boundaries and dynamic memory allocation.

## Definition

```c
ItemPointer
ginPostingListDecodeAllSegments(GinPostingList *segment, int len, int *ndecoded_out)
```
## Detailed Description
This function is the core decoder for GIN posting lists, capable of processing multiple consecutive segments within a given byte length. It handles the complex task of decoding variable-byte encoded deltas back into absolute ItemPointer values while managing dynamic memory allocation for the output array.

The function processes each segment by first copying the segment's base ItemPointer (stored as 'first'), then decoding the subsequent delta-encoded values. Each delta is decoded using variable-byte encoding and added to the running value to reconstruct the original ItemPointers. The algorithm maintains sorted order and dynamically grows the output array as needed.

The function supports processing multiple segments stored consecutively in memory, making it suitable for decoding entire posting lists that may span multiple segments due to size limitations.

## Parameters / Member Variables
- : Pointer to the first posting list segment to decode
- : Total number of bytes containing all segments to process
- : Output parameter that receives the total number of items decoded across all segments

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [repalloc](../r/repalloc.md) (memory reallocation) 
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - OffsetNumberIsValid
  - [ginCompareItemPointers](ginCompareItemPointers.md)
  - [itemptr_to_uint64](../i/itemptr_to_uint64.md)
  - [decode_varbyte](../d/decode_varbyte.md)
  - [uint64_to_itemptr](../u/uint64_to_itemptr.md)
  - GinNextPostingListSegment
- Called from (representative examples):
  - [GinDataLeafPageGetItems](../G/GinDataLeafPageGetItems.md) (gin/gindatapage.c:160)
  - [ginPostingListDecode](ginPostingListDecode.md) (gin/ginpostinglist.c:286)
  - [ginPostingListDecodeAllSegmentsToTbm](ginPostingListDecodeAllSegmentsToTbm.md) (gin/ginpostinglist.c:364)

## Notes and Other Information
- Dynamically allocates and grows the output array starting with an initial guess based on the first segment size
- Maintains strict ordering requirements - each decoded ItemPointer must be greater than the previous one
- Uses variable-byte encoding to decode compressed deltas efficiently
- The returned ItemPointer array must be freed by the caller using pfree()
- Critical for GIN index performance as it handles the decompression of stored tuple identifiers
- Supports processing multiple segments in a single call, enabling efficient batch decoding operations