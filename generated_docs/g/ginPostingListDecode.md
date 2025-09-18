# ginPostingListDecode

## Location
[src/backend/access/gin/ginpostinglist.c:284-296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginpostinglist.c#L284-L296)

## Overview
A convenience wrapper function that decodes a complete compressed GIN posting list into an array of item pointers.

## Definition


## Detailed Description
This function serves as a simplified interface for decoding compressed GIN posting lists. It internally calls  with the full size of the posting list to decode all segments at once. The function is designed to handle the common case where the entire posting list needs to be decoded, abstracting away the complexity of partial segment decoding.

The function takes a compressed posting list and returns a newly allocated array of ItemPointers representing the decoded tuple identifiers. The caller is responsible for freeing the returned array.

## Parameters / Member Variables
- : Pointer to the compressed GIN posting list to decode
- : Output parameter that receives the number of items decoded

## Dependencies
- Functions called/Symbols referenced:
  - [ginPostingListDecodeAllSegments](ginPostingListDecodeAllSegments.md)
  - SizeOfGinPostingList
  - [GinPostingList](../G/GinPostingList.md) (type)
- Called from (representative examples):
  - [dataBeginPlaceToPageLeaf](../d/dataBeginPlaceToPageLeaf.md) (gin/gindatapage.c:504, 677)
  - [ginVacuumPostingTreeLeaf](ginVacuumPostingTreeLeaf.md) (gin/gindatapage.c:756)
  - [addItemsToLeaf](../a/addItemsToLeaf.md) (gin/gindatapage.c:1503)
  - [leafRepackItems](../l/leafRepackItems.md) (gin/gindatapage.c:1652, 1655)
  - [ginReadTuple](ginReadTuple.md) (gin/ginentrypage.c:174)
  - [ginCompressPostingList](ginCompressPostingList.md) (gin/ginpostinglist.c:268)

## Notes and Other Information
- This is a wrapper function that simplifies the common use case of decoding an entire posting list
- The returned ItemPointer array must be freed by the caller using pfree()
- The function handles all segments of the posting list automatically by determining the total size
- Used extensively throughout the GIN access method for various operations including vacuum, page reorganization, and tuple reading