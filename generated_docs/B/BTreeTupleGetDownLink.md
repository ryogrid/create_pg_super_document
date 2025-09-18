# BTreeTupleGetDownLink

## Location
src/include/access/nbtree.h: 556 - 561

## Overview
Retrieves the downlink block number from a pivot tuple in a B-tree structure, extracting the block pointer that references a child page.

## Definition
static inline BlockNumber BTreeTupleGetDownLink(IndexTuple pivot)

## Detailed Description
This function extracts the downlink block number from a pivot tuple's ItemPointer (t_tid field). In B-tree internal pages, pivot tuples contain downlinks that point to child pages. The function uses ItemPointerGetBlockNumberNoCheck() to retrieve the block number without performing validation checks, as noted in the comment that assertion checks for pivot tuples would cause false positives in !heapkeyspace indexes.

The function is implemented as a static inline function for performance, as it's frequently called during B-tree traversal operations.

## Parameters / Member Variables
- pivot: IndexTuple representing a pivot tuple from which to extract the downlink block number

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerGetBlockNumberNoCheck](../I/ItemPointerGetBlockNumberNoCheck.md)
- Called from (representative examples):
  - [_bt_getstackbuf](../b/_bt_getstackbuf.md)
  - [_bt_mark_page_halfdead](../b/_bt_mark_page_halfdead.md)
  - [_bt_unlink_halfdead_page](../b/_bt_unlink_halfdead_page.md)
  - [_bt_search](../b/_bt_search.md)
  - [_bt_get_endpoint](../b/_bt_get_endpoint.md)
  - [btree_xlog_mark_page_halfdead](../b/btree_xlog_mark_page_halfdead.md)

## Notes and Other Information
- The function does not assert that the input tuple is actually a pivot tuple to avoid false positive assertion failures in !heapkeyspace indexes
- This is a performance-critical function implemented as static inline
- Used extensively during B-tree navigation, page splitting, deletion, and WAL recovery operations
- The downlink information is stored in the tuple's t_tid field, which normally stores heap tuple location but is repurposed in pivot tuples to store child page references