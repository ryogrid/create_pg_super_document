# BTreeTupleGetPostingN

## Location
src/include/access/nbtree.h: 544 - 555

## Overview
BTreeTupleGetPostingN is an inline function that returns a pointer to the Nth heap TID in a posting list tuple.

## Definition
static inline ItemPointer BTreeTupleGetPostingN(IndexTuple posting, int n)

## Detailed Description
This function provides indexed access to individual heap tuple identifiers within a posting list tuple. It combines the functionality of BTreeTupleGetPosting with pointer arithmetic to directly access the heap TID at the specified index position. This is essential for operations that need to examine or manipulate specific heap TIDs within a posting list, such as during uniqueness checking, vacuum operations, or when splitting posting lists in PostgreSQL's B-tree implementation.

## Parameters / Member Variables
- posting: The IndexTuple that is a posting list tuple containing multiple heap TIDs
- n: The zero-based index of the heap TID to access within the posting list

## Dependencies
- Functions called/Symbols referenced:
  - BTreeTupleGetPosting
- Called from (representative examples):
  - _bt_bottomupdel_finish_pending
  - _bt_update_posting
  - _bt_swap_posting
  - _bt_check_unique
  - _bt_simpledel_pass
  - _bt_binsrch_posting
  - _bt_readpage
  - BTreeTupleGetMaxHeapTID

## Notes and Other Information
- The caller must ensure that n is within valid bounds (0 <= n < BTreeTupleGetNPosting(posting))
- Returns a pointer to an ItemPointer structure representing the heap TID
- Commonly used in loops to iterate through all heap TIDs in a posting list
- Essential for operations that need random access to posting list elements
- Used extensively in B-tree maintenance operations and search routines