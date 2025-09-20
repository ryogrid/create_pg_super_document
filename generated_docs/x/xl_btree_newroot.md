# xl_btree_newroot

## Location
[src/include/access/nbtxlog.h:344-348](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtxlog.h#L344-L348)

## Overview
WAL record structure for creating a new root page in a B-tree, either establishing an empty root for a new index or creating a new root level after splitting the previous root.

## Definition

```c
typedef struct xl_btree_newroot
{
	BlockNumber rootblk;		/* location of new root (redundant with blk 0) */
	uint32		level;			/* its tree level */
} xl_btree_newroot;
```
## Detailed Description
The xl_btree_newroot structure represents the creation of a new root page in a B-tree index. This operation occurs in two scenarios: creating the very first root page for an empty index, or creating a new root level when the existing root page is split. The structure is minimal because most information is embedded in the backup blocks and can be derived from the level field.

When creating the first root page (level 0), the new root is both a leaf and root page with no tuples. When splitting an existing root, the new root becomes a non-leaf page at level+1, containing two downlink tuples: one pointing to the old root (left child) with a minus-infinity key, and one pointing to the new right sibling with the high key from the old root. The metadata page is updated to reflect the new root location and level.

## Parameters / Member Variables
- `rootblk`: Block number of the newly created root page (redundant with backup block 0, included for clarity)
- `level`: Level of the new root page in the tree (0 for leaf-only trees, >0 for internal nodes)
## Dependencies
- Functions called/Symbols referenced:
  - SizeOfBtreeNewroot (size calculation macro)  
  - xl_btree_metadata (embedded in backup block data)
- Called from (representative examples):
  - [_bt_newlevel](../b/_bt_newlevel.md) (src/backend/access/nbtree/nbtinsert.c:2559)
  - [_bt_getroot](../b/_bt_getroot.md) (src/backend/access/nbtree/nbtpage.c:474)
  - [btree_xlog_newroot](../b/btree_xlog_newroot.md) (src/backend/access/nbtree/nbtxlog.c:940)
  - [btree_desc](../b/btree_desc.md) (src/backend/access/rmgrdesc/nbtdesc.c:108)

## Notes and Other Information
- The record includes up to 3 backup blocks: new root page, left child (if splitting), and metapage
- When level=0, establishes the first root page for an empty index (leaf+root)
- When level>0, creates a new root level after splitting the old root page
- For root splits, the payload contains two tuples: minus-infinity key pointing to old root, and high key pointing to new right sibling
- The metapage is updated with new root location, level, fastroot, and fastlevel values
- Recovery clears the BTP_INCOMPLETE_SPLIT flag in the left child when creating a new root from a split
- Unlike other B-tree WAL records, this doesn't need a separate xl_btree_metadata record since metadata changes are implicit