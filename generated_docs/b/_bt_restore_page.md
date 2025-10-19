# _bt_restore_page

## Location
[src/backend/access/nbtree/nbtxlog.c:36-81](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtxlog.c#L36-L81)

## Overview
Re-enters all index tuples on a freshly initialized B-tree page during WAL replay operations.

## Definition

```c
static void
_bt_restore_page(Page page, char *from, int len)
```
## Detailed Description
This function is part of PostgreSQL's B-tree WAL (Write Ahead Logging) recovery mechanism. It takes a freshly initialized page and restores all the index tuples from a buffer containing the upper part of the original page (from pd_upper to pd_special). 

The function assumes that tuples were originally added to the page in item-number order, with the highest item number appearing first (lowest position on the page). To restore the original order, the function first scans through the buffer in forward order to identify individual tuples, then adds them to the page in reverse order.

The restoration process involves careful memory handling since the items in the buffer may not be properly aligned, requiring the use of memcpy() for safe access.

## Parameters / Member Variables
- `page`: The freshly initialized page where tuples will be restored
- `*from`: Pointer to buffer containing the saved upper part of the original page
- `len`: Length of the buffer in bytes
## Dependencies
- Functions called/Symbols referenced:
  - IndexTupleSize
  - PageAddItem
  - MAXALIGN
  - elog (PANIC level)
- Data types used:
  - [IndexTupleData](../I/IndexTupleData.md)
  - Item
  - MaxIndexTuplesPerPage
  - InvalidOffsetNumber
- Called from (representative examples):
  - [btree_xlog_split](btree_xlog_split.md)
  - [btree_xlog_newroot](btree_xlog_newroot.md)

## Notes and Other Information
- This is a static function used internally within nbtxlog.c for B-tree WAL recovery
- Uses careful alignment handling with MAXALIGN() and memcpy() to handle potentially unaligned data
- Will panic if unable to add an item to the page, indicating a serious recovery error
- The reverse-order insertion is critical for maintaining the original tuple ordering
- Limited to MaxIndexTuplesPerPage items per restoration operation

## Simplified Source

```c
static void _bt_restore_page(Page page, char *from, int len)
{
    IndexTupleData itupdata;
    Size itemsz;
    char *end = from + len;
    Item items[MaxIndexTuplesPerPage];
    uint16 itemsizes[MaxIndexTuplesPerPage];
    int nitems = 0;

    // First pass: scan forward to identify all tuples
    while (from < end)
    {
        // Copy tuple header safely (may not be aligned)
        memcpy(&itupdata, from, sizeof(IndexTupleData));
        itemsz = IndexTupleSize(&itupdata);
        itemsz = MAXALIGN(itemsz);

        // Store tuple info for later insertion
        items[nitems] = (Item) from;
        itemsizes[nitems] = itemsz;
        nitems++;

        from += itemsz;
    }

    // Second pass: add tuples in reverse order to maintain original ordering
    for (int i = nitems - 1; i >= 0; i--)
    {
        if (PageAddItem(page, items[i], itemsizes[i], nitems - i, false, false) == InvalidOffsetNumber)
            elog(PANIC, "_bt_restore_page: cannot add item to page");
    }
}
```