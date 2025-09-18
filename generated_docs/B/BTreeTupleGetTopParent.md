# BTreeTupleGetTopParent

## Location
[src/include/access/nbtree.h:620-625](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L620-L625)

## Overview
Retrieves the "top parent" block number from a leaf page's high key tuple, which is used during B-tree page deletion operations to track parent page relationships.

## Definition
static inline BlockNumber BTreeTupleGetTopParent(IndexTuple leafhikey)

## Detailed Description
This function extracts the "top parent" block number from a leaf page's high key tuple. The top parent link is a special mechanism used during page deletion to track the relationship between a leaf page and its parent page in the B-tree structure. The function uses ItemPointerGetBlockNumberNoCheck() to retrieve the block number stored in the tuple's t_tid field without performing validation checks.

Similar to other B-tree tuple functions, it avoids asserting that the tuple is a pivot tuple to prevent false positive assertion failures in !heapkeyspace indexes. The top parent information is crucial for maintaining B-tree structural integrity during deletion operations.

## Parameters / Member Variables
- leafhikey: IndexTuple representing a leaf page's high key tuple from which to extract the top parent block number

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerGetBlockNumberNoCheck](../I/ItemPointerGetBlockNumberNoCheck.md)
- Called from (representative examples):
  - [_bt_unlink_halfdead_page](../b/_bt_unlink_halfdead_page.md)

## Notes and Other Information
- Used specifically during B-tree page deletion operations to maintain parent-child relationships
- The "top parent" concept is part of the B-tree deletion algorithm to ensure proper unlinking of pages
- Does not assert that the input is a pivot tuple to avoid false positives in !heapkeyspace indexes
- The function operates on a leaf page's high key tuple, which temporarily stores parent page information during deletion
- Implemented as static inline for performance during deletion operations
- Works in conjunction with BTreeTupleSetTopParent to manage top parent links during page deletion sequences