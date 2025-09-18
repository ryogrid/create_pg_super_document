# PageIndexTupleDelete

## Location
src/backend/storage/page/bufpage.c: 1052 - 1160

## Overview
Removes a tuple from an index page by compacting out the line pointer and adjusting all remaining data structures accordingly.

## Definition


## Detailed Description
PageIndexTupleDelete performs the complete removal of a tuple from an index page, which involves more complex operations than heap tuple deletion. Unlike heap pages where line pointers are typically left in place when tuples are deleted, index pages compact out the deleted line pointer to maintain efficiency.

The function performs extensive validation to ensure page integrity, checking page header boundaries and line pointer validity. After validation, it removes the specified tuple by:

1. Validating the page structure and offset number
2. Retrieving tuple information (size, offset) from the line pointer
3. Removing the line pointer from the pd_linp array by shifting subsequent entries
4. Moving tuple data to eliminate the gap left by the deleted tuple
5. Adjusting page boundaries (pd_upper, pd_lower)
6. Updating remaining line pointers whose offsets were affected by the data movement

This comprehensive approach ensures that index pages remain compact and efficient after deletions, unlike heap pages where deleted space may remain fragmented.

## Parameters / Member Variables
- : A pointer to the index page from which to delete the tuple
- : The offset number of the tuple to delete

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader
  - ItemId
  - SizeOfPageHeaderData
  - ERRCODE_DATA_CORRUPTED
  - PageGetMaxOffsetNumber
  - PageGetItemId
  - ItemIdHasStorage
  - ItemIdGetLength
  - ItemIdGetOffset
  - ItemIdData
  - PageIsEmpty
- Called from (representative examples):
  - entryPreparePage
  - ginVacuumEntryPage
  - ginRedoInsertEntry
  - gistplacetopage
  - gistdeletepage
  - gistRedoPageDelete
  - _bt_mark_page_halfdead
  - btree_xlog_mark_page_halfdead
  - addLeafTuple
  - spgAddNodeAction
  - spgSplitNodeAction
  - SpGistPageAddNewItem
  - PageIndexMultiDelete

## Notes and Other Information
- Designed specifically for index pages, not heap pages
- Compacts out deleted line pointers, unlike heap tuple deletion
- Performs extensive page integrity validation before proceeding
- Handles data movement and line pointer offset adjustments automatically
- Updates page boundaries (pd_upper, pd_lower) to reflect the deletion
- More complex than heap deletion due to index page structure requirements
- Maintains page compactness by eliminating gaps left by deleted tuples
- Located in src/backend/storage/page/bufpage.c:1052-1160