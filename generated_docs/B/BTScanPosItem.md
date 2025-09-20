# BTScanPosItem

## Location
[src/include/access/nbtree.h:944-949](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L944-L949)

## Overview
BTScanPosItem is a structure that stores information about each matching item found during a B-tree index scan, including heap TID, index offset, and tuple workspace location.

## Definition

```c
typedef struct BTScanPosItem	/* what we remember about each match */
{
	ItemPointerData heapTid;	/* TID of referenced heap item */
	OffsetNumber indexOffset;	/* index item's location within page */
	LocationIndex tupleOffset;	/* IndexTuple's offset in workspace, if any */
} BTScanPosItem;
```
## Detailed Description
This structure represents what the B-tree access method remembers about each matching item during an index scan. It is part of the page-at-a-time scanning approach where the system pins and read-locks a page, identifies all matching items, saves them in BTScanPosItem structures, then releases the read-lock while returning items to the caller. This minimizes lock/unlock traffic while maintaining necessary synchronization for VACUUM operations.

## Parameters / Member Variables
- : ItemPointerData containing the TID (tuple identifier) of the referenced heap item
- : OffsetNumber specifying the index item's location within the current page
- : LocationIndex indicating the IndexTuple's offset in the workspace array (used for index-only scans)

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerData](../I/ItemPointerData.md)
  - OffsetNumber
  - LocationIndex
- Called from (representative examples):
  - [btrestrpos](../b/btrestrpos.md)
  - [_bt_first](../b/_bt_first.md)
  - [_bt_next](../b/_bt_next.md)
  - [_bt_saveitem](../b/_bt_saveitem.md)
  - [_bt_setuppostingitems](../b/_bt_setuppostingitems.md)
  - [_bt_savepostingitem](../b/_bt_savepostingitem.md)
  - [_bt_steppage](../b/_bt_steppage.md)
  - [_bt_endpoint](../b/_bt_endpoint.md)
  - [_bt_killitems](../b/_bt_killitems.md)
  - [BTScanPosData](BTScanPosData.md)

## Notes and Other Information
- Used in both regular index scans and index-only scans
- For index-only scans, the entire IndexTuple is saved in a separate workspace array
- For posting list tuples, a base tuple is stored once and shared across multiple TIDs
- Part of the page-at-a-time scanning strategy that optimizes lock usage
- Essential for VACUUM synchronization mechanisms in B-tree operations