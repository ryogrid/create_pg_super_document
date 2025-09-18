# entryLocateLeafEntry

## Location
[src/backend/access/gin/ginentrypage.c:346-404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginentrypage.c#L346-L404)

## Overview
Searches for the correct position of a value on a GIN index leaf page using binary search, returning whether the exact value was found.

## Definition


## Detailed Description
This function performs a binary search on a leaf page of a GIN index to locate the correct position for a specific entry value. Unlike entryLocateEntry which works on non-leaf pages to find child pages, this function operates on leaf pages to find actual data entries. It assumes the page has been correctly chosen during the index traversal.

The function returns true if an exact match is found, and false otherwise. In both cases, it updates the stack offset to point to the appropriate position: either the exact match location or the insertion point where the value should be placed. For full scans, it simply sets the offset to the first position and returns true.

## Parameters / Member Variables
- : GinBtree structure containing search parameters including the entry key, attribute number, and category being searched for
- : GinBtreeStack structure representing the current leaf page position, which will be updated with the offset of the located or insertion position

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - GinPageIsLeaf
  - GinPageIsData
  - FirstOffsetNumber
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [gintuple_get_attrnum](../g/gintuple_get_attrnum.md)
  - [gintuple_get_key](../g/gintuple_get_key.md)
  - [ginCompareAttEntries](../g/ginCompareAttEntries.md)
- Called from (representative examples):
  - [ginPrepareEntryScan](../g/ginPrepareEntryScan.md)

## Notes and Other Information
- This is a static function internal to the GIN entry page implementation
- The function assumes the input page is a leaf, non-data page (verified by assertions)
- Uses binary search for efficient lookup in sorted leaf pages
- Returns boolean indicating exact match found (true) or not found (false)
- Always updates stack offset to the correct position regardless of match result
- For empty pages (high < low), sets offset to FirstOffsetNumber and returns false
- Critical for both search operations and determining insertion points for new entries