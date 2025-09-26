# BTPageState

## Location
[src/backend/access/nbtree/nbtsort.c:229-239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L229-L239)

## Overview
BTPageState is a status record structure that represents a B-tree page being built during index construction, with one instance maintained for each active tree level.

## Definition

```c
typedef struct BTPageState
{
	BulkWriteBuffer btps_buf;	/* workspace for page building */
	BlockNumber btps_blkno;		/* block # to write this page at */
	IndexTuple	btps_lowkey;	/* page's strict lower bound pivot tuple */
	OffsetNumber btps_lastoff;	/* last item offset loaded */
	Size		btps_lastextra; /* last item's extra posting list space */
	uint32		btps_level;		/* tree level (0 = leaf) */
	Size		btps_full;		/* "full" if less than this much free space */
	struct BTPageState *btps_next;	/* link to parent level, if any */
} BTPageState;
```
## Detailed Description
BTPageState manages the construction state of individual B-tree pages during index building operations. Each active tree level maintains its own BTPageState instance, forming a linked list structure through the btps_next pointer. The structure tracks the page's position in the tree, its content boundaries, and workspace for building the page before writing it to storage.

The structure supports both leaf and internal pages at different tree levels, with level 0 representing leaf pages. It maintains space management information to determine when a page is considered "full" and ready to be written, along with tracking the last loaded item and any extra space requirements for posting lists.

## Parameters / Member Variables
- `btps_buf`: BulkWriteBuffer workspace used for constructing the page content before writing
- `btps_blkno`: Block number where this page will be written in the relation
- `btps_lowkey`: Index tuple representing the page's strict lower bound pivot tuple for navigation
- `btps_lastoff`: Offset number of the last item that was loaded onto this page
- `btps_lastextra`: Size of extra space used by the last item's posting list (for compressed tuples)
- `btps_level`: Tree level indicator where 0 represents leaf pages and higher numbers represent internal pages
- `btps_full`: Size threshold - page is considered "full" when free space falls below this value
- `*btps_next`: Pointer to the BTPageState of the parent level, forming a linked list of page states up the tree
## Dependencies
- Functions called/Symbols referenced:
  - BulkWriteBuffer
  - [BTPageState](BTPageState.md) (self-reference for linked list)
- Called from (representative examples):
  - [_bt_blwritepage](../b/_bt_blwritepage.md)
  - [_bt_pagestate](../b/_bt_pagestate.md)
  - [_bt_buildadd](../b/_bt_buildadd.md)
  - [_bt_sort_dedup_finish_pending](../b/_bt_sort_dedup_finish_pending.md)
  - [_bt_uppershutdown](../b/_bt_uppershutdown.md)
  - [_bt_load](../b/_bt_load.md)

## Notes and Other Information
BTPageState forms a linked list structure representing the active page being built at each level of the B-tree, with btps_next pointing toward parent levels. The structure is essential for bottom-up B-tree construction where leaf pages are built first, followed by internal pages as the tree grows upward. The btps_full threshold helps optimize page utilization by determining when to close a page and start a new one. The workspace buffer (btps_buf) allows pages to be constructed in memory before being written to storage, supporting efficient bulk loading operations.