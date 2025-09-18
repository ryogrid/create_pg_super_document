# entryLocateEntry

## Location
[src/backend/access/gin/ginentrypage.c:270-345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginentrypage.c#L270-L345)

## Overview
Finds the correct tuple in a non-leaf GIN index page using binary search to locate the appropriate child page to descend to during index traversal.

## Definition


## Detailed Description
This function performs a binary search on a non-leaf GIN index page to find the correct child page to descend to during index traversal. It operates on the assumption that the page has been correctly chosen and that the searching value should be present on the page. The function handles both full scan operations and targeted searches, comparing attribute entries using the GIN comparison functions to determine the correct downlink to follow.

For full scans, it simply returns the leftmost child page. For targeted searches, it performs a binary search algorithm, comparing the search key with entries on the page to find either an exact match or the appropriate position where the key should be located. The function properly handles the right infinity case when the search reaches the rightmost entry of a rightmost page.

## Parameters / Member Variables
- : GinBtree structure containing search parameters and callback functions for the current GIN index operation
- : GinBtreeStack structure representing the current position in the index traversal, which will be updated with the offset of the located entry

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - GinPageIsLeaf  
  - GinPageIsData
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - FirstOffsetNumber
  - GinPageRightMost
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [gintuple_get_attrnum](../g/gintuple_get_attrnum.md)
  - [gintuple_get_key](../g/gintuple_get_key.md)
  - [ginCompareAttEntries](../g/ginCompareAttEntries.md)
  - GinGetDownlink
- Called from (representative examples):
  - [ginPrepareEntryScan](../g/ginPrepareEntryScan.md)

## Notes and Other Information
- This is a static function internal to the GIN entry page implementation
- The function assumes the input page is a non-leaf, non-data page (verified by assertions)
- Uses binary search algorithm for efficient lookup in sorted index pages
- Handles special case of right infinity for rightmost pages
- Returns the block number of the child page to descend to next
- Updates the stack offset to point to the located entry for continued traversal