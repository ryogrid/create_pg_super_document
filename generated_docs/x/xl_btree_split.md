# xl_btree_split

## Location
[src/include/access/nbtxlog.h:153-159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtxlog.h#L153-L159)

## Overview
The xl_btree_split structure represents a WAL record for B-tree page split operations, containing metadata necessary for recovery to reconstruct both left and right pages after a split.

## Definition
```c
typedef struct xl_btree_split
{
    uint32      level;          /* tree level of page being split */
    OffsetNumber firstrightoff; /* first origpage item on rightpage */
    OffsetNumber newitemoff;    /* new item's offset */
    uint16      postingoff;     /* offset inside orig posting tuple */
} xl_btree_split;
```

## Detailed Description
This structure logs B-tree page splits, one of the most complex operations in B-tree maintenance. Page splits occur when a page becomes too full to accommodate a new item. The structure supports two variants: XLOG_BTREE_SPLIT_L (new item goes to left page) and XLOG_BTREE_SPLIT_R (new item goes to right page).

The WAL record uses an optimized approach where all items for the right sibling are saved completely, while the left page uses incremental updates. This reduces WAL space since XLogInsert would typically store the entire right page image. The record includes backup blocks for the original/left page, new right page, next block, and child's left sibling for non-leaf splits.

Special handling exists for posting list splits, where the record may contain an orignewitem (the item before posting list split) rather than the final newitem, allowing recovery to properly reconstruct posting lists.

## Parameters / Member Variables
- `level`: The tree level of the page being split (0 for leaf pages, higher for internal pages)
- `firstrightoff`: Offset of the first item from the original page that goes to the right page
- `newitemoff`: Offset where the new item should be inserted
- `postingoff`: Offset within a posting tuple for posting list splits (0 if not a posting split)

## Dependencies
- Functions called/Symbols referenced:
  - uint32 (type)
  - OffsetNumber (type)
  - uint16 (type)

- Called from (representative examples):
  - [_bt_split](../b/_bt_split.md) (src/backend/access/nbtree/nbtinsert.c:1969)
  - [btree_xlog_split](../b/btree_xlog_split.md) (src/backend/access/nbtree/nbtxlog.c:254)
  - [btree_desc](../b/btree_desc.md) (src/backend/access/rmgrdesc/nbtdesc.c:44)
  - SizeOfBtreeSplit (src/include/access/nbtxlog.h:161)

## Notes and Other Information
- Supports two operation variants: SPLIT_L (new item to left page) and SPLIT_R (new item to right page)
- Uses optimized logging strategy: complete right page reconstruction vs. incremental left page updates
- Backup blocks include: original/left page (Blk 0), right page (Blk 1), next block (Blk 2), child's left sibling for non-leaf (Blk 3)
- [Complex](../C/Complex.md) posting list split handling where postingoff indicates need for posting list reconstruction
- Left page's high key is always logged due to suffix truncation and user-defined code requirements
- The postingoff field being non-zero indicates REDO must reconstruct posting list tuple for left page
- Equivalent to xl_btree_insert's INSERT_POST handling but more complex due to page split context