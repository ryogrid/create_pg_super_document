# PageIndexTupleDeleteNoCompact

## Location
src/backend/storage/page/bufpage.c: 1295 - 1404

## Overview
Removes a specified tuple from an index page by marking its line pointer as unused rather than compacting it out, preserving existing tuple identifier (TID) stability.

## Definition


## Detailed Description
PageIndexTupleDeleteNoCompact provides a specialized deletion mechanism for index access methods that require existing TIDs of live tuples to remain unchanged. Instead of compacting out the deleted tuple and shifting line pointers (which would change TIDs), this function:

1. Marks the line pointer as "unused" if it's not the last one on the page
2. Removes the line pointer entirely only if it's the last one (safe for TID preservation)
3. Moves tuple data to reclaim space while adjusting remaining line pointer offsets
4. Maintains page structure integrity without TID renumbering

This approach trades some space efficiency for TID stability, which is crucial for certain index types that rely on persistent tuple references.

## Parameters / Member Variables
- : The index page containing the tuple to delete
- : The offset number (line pointer index) of the tuple to delete

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetMaxOffsetNumber](PageGetMaxOffsetNumber.md)
  - [PageGetItemId](PageGetItemId.md)
  - ItemIdHasStorage
  - ItemIdGetLength
  - ItemIdGetOffset
  - ItemIdSetUnused
  - [PageIsEmpty](PageIsEmpty.md)
- Called from (representative examples):
  - [brin_doupdate](../b/brin_doupdate.md) (BRIN index tuple updates)
  - [brinRevmapDesummarizeRange](../b/brinRevmapDesummarizeRange.md) (BRIN revmap operations)
  - [brin_xlog_update](../b/brin_xlog_update.md) (BRIN WAL replay)
  - [brin_xlog_desummarize_page](../b/brin_xlog_desummarize_page.md) (BRIN WAL desummarization)

## Notes and Other Information
- Primary use case: Index access methods requiring TID stability (notably BRIN indexes)
- Unlike PageIndexTupleDelete, does not compact line pointer array to preserve TID values
- Only removes line pointer if it's the last one on the page (safe operation)
- Reclaims tuple space by moving data and adjusting remaining line pointer offsets
- Includes comprehensive page corruption validation
- More space-wasteful than compact deletion but necessary for TID-sensitive operations
- Line pointers marked as unused can potentially be reused for future insertions