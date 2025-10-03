# PageIndexTupleDelete

## Location
[src/backend/storage/page/bufpage.c:1052-1160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/page/bufpage.c#L1052-L1160)

## Overview
Removes a tuple from an index page by compacting out the line pointer and adjusting all remaining data structures accordingly.

## Definition

```c
void
PageIndexTupleDelete(Page page, OffsetNumber offnum)
```
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
- `page`: A pointer to the index page from which to delete the tuple
- `offnum`: The offset number of the tuple to delete
## Dependencies
- Functions called/Symbols referenced:
  - PageHeader
  - ItemId
  - SizeOfPageHeaderData
  - ERRCODE_DATA_CORRUPTED
  - [PageGetMaxOffsetNumber](PageGetMaxOffsetNumber.md)
  - [PageGetItemId](PageGetItemId.md)
  - ItemIdHasStorage
  - ItemIdGetLength
  - ItemIdGetOffset
  - [ItemIdData](../I/ItemIdData.md)
  - [PageIsEmpty](PageIsEmpty.md)
- Called from (representative examples):
  - [entryPreparePage](../e/entryPreparePage.md)
  - [ginVacuumEntryPage](../g/ginVacuumEntryPage.md)
  - [ginRedoInsertEntry](../g/ginRedoInsertEntry.md)
  - [gistplacetopage](../g/gistplacetopage.md)
  - [gistdeletepage](../g/gistdeletepage.md)
  - [gistRedoPageDelete](../g/gistRedoPageDelete.md)
  - [_bt_mark_page_halfdead](../b/_bt_mark_page_halfdead.md)
  - [btree_xlog_mark_page_halfdead](../b/btree_xlog_mark_page_halfdead.md)
  - [addLeafTuple](../a/addLeafTuple.md)
  - [spgAddNodeAction](../s/spgAddNodeAction.md)
  - [spgSplitNodeAction](../s/spgSplitNodeAction.md)
  - [SpGistPageAddNewItem](../S/SpGistPageAddNewItem.md)
  - [PageIndexMultiDelete](PageIndexMultiDelete.md)

## Notes and Other Information
- Designed specifically for index pages, not heap pages
- Compacts out deleted line pointers, unlike heap tuple deletion
- Performs extensive page integrity validation before proceeding
- Handles data movement and line pointer offset adjustments automatically
- Updates page boundaries (pd_upper, pd_lower) to reflect the deletion
- More complex than heap deletion due to index page structure requirements
- Maintains page compactness by eliminating gaps left by deleted tuples
- Located in src/backend/storage/page/bufpage.c:1052-1160