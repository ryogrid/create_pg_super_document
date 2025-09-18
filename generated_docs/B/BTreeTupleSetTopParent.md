# BTreeTupleSetTopParent

## Location
src/include/access/nbtree.h: 626 - 637

## Overview
Sets the "top parent" block number in a leaf page's high key tuple and configures it as a pivot tuple, used during B-tree page deletion operations to establish parent page relationships.

## Definition
static inline void BTreeTupleSetTopParent(IndexTuple leafhikey, BlockNumber blkno)

## Detailed Description
This function establishes the "top parent" link in a leaf page's high key tuple during B-tree page deletion operations. It performs two key operations: first, it sets the specified block number in the tuple's ItemPointer using ItemPointerSetBlockNumber(), then it calls BTreeTupleSetNAtts() with parameters (0, false) to configure the tuple as a pivot tuple with zero key attributes and no heap TID tiebreaker.

The top parent mechanism is part of PostgreSQL's B-tree deletion algorithm, which temporarily repurposes a leaf page's high key to store parent page information during the deletion process. This ensures proper maintenance of the B-tree structure during page unlinking operations.

## Parameters / Member Variables
- leafhikey: IndexTuple representing a leaf page's high key tuple to modify
- blkno: BlockNumber of the parent page to store as the top parent reference

## Dependencies
- Functions called/Symbols referenced:
  - ItemPointerSetBlockNumber
  - BTreeTupleSetNAtts
- Called from (representative examples):
  - _bt_mark_page_halfdead
  - _bt_unlink_halfdead_page
  - btree_xlog_mark_page_halfdead
  - btree_xlog_unlink_page

## Notes and Other Information
- This is the counterpart to BTreeTupleGetTopParent, used for setting rather than retrieving top parent information
- Used exclusively during B-tree page deletion operations to maintain parent-child relationships
- The function transforms a regular high key into a special pivot tuple by calling BTreeTupleSetNAtts with zero attributes
- Essential for the B-tree deletion algorithm's ability to track page relationships during unlinking
- Used in both normal deletion operations and WAL recovery scenarios
- The top parent link is temporary and exists only during the deletion process to ensure structural integrity