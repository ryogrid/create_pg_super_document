# BTreeTupleSetDownLink

## Location
[src/include/access/nbtree.h:562-576](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L562-L576)

## Overview
Sets the downlink block number in a pivot tuple, establishing the connection between an internal B-tree page and its child page.

## Definition
static inline void BTreeTupleSetDownLink(IndexTuple pivot, BlockNumber blkno)

## Detailed Description
This function sets the downlink block number in a pivot tuple's ItemPointer (t_tid field). In B-tree internal pages, pivot tuples contain downlinks that point to child pages. The function uses ItemPointerSetBlockNumber() to store the specified block number in the tuple's t_tid field, which is repurposed from its normal role of storing heap tuple location to instead store child page references in pivot tuples.

The function is implemented as a static inline function for performance, as it's used during B-tree construction, page splitting, and structural modifications.

## Parameters / Member Variables
- pivot: IndexTuple representing a pivot tuple to modify
- blkno: BlockNumber specifying the block number of the child page to link to

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerSetBlockNumber](../I/ItemPointerSetBlockNumber.md)
- Called from (representative examples):
  - [_bt_insert_parent](../b/_bt_insert_parent.md)
  - [_bt_newlevel](../b/_bt_newlevel.md)
  - [_bt_mark_page_halfdead](../b/_bt_mark_page_halfdead.md)
  - [_bt_buildadd](../b/_bt_buildadd.md)
  - [_bt_uppershutdown](../b/_bt_uppershutdown.md)
  - [btree_xlog_mark_page_halfdead](../b/btree_xlog_mark_page_halfdead.md)

## Notes and Other Information
- This is the counterpart to BTreeTupleGetDownLink, used for modifying rather than reading downlink information
- Used during B-tree construction, page splitting operations, and structural modifications
- The function directly modifies the pivot tuple's t_tid field to establish parent-child relationships in the B-tree structure
- Performance-critical function implemented as static inline for efficiency during bulk operations and frequent structural changes

## Simplified Source

```c
static inline void
BTreeTupleSetDownLink(IndexTuple pivot, BlockNumber blkno)
{
    // Set the child page block number in pivot tuple's t_tid field
    ItemPointerSetBlockNumber(&pivot->t_tid, blkno);
}
```