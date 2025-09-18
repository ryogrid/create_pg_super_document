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
  - ItemPointerGetBlockNumberNoCheck
- Called from (representative examples):
  - _bt_getstackbuf
  - _bt_mark_page_halfdead
  - _bt_unlink_halfdead_page
  - _bt_search
  - _bt_get_endpoint
  - btree_xlog_mark_page_halfdead

## Notes and Other Information
- The function does not assert that the input tuple is actually a pivot tuple to avoid false positive assertion failures in !heapkeyspace indexes
- This is a performance-critical function implemented as static inline
- Used extensively during B-tree navigation, page splitting, deletion, and WAL recovery operations
- The downlink information is stored in the tuple's t_tid field, which normally stores heap tuple location but is repurposed in pivot tuples to store child page references