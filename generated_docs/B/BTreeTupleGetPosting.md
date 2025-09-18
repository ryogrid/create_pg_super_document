# BTreeTupleGetPosting

## Location
src/include/access/nbtree.h: 537 - 543

## Overview
BTreeTupleGetPosting is an inline function that returns a pointer to the posting list array contained within a posting list tuple.

## Definition
static inline ItemPointer BTreeTupleGetPosting(IndexTuple posting)

## Detailed Description
This function calculates and returns a pointer to the beginning of the heap TID array within a posting list tuple. It accomplishes this by adding the posting offset (obtained from BTreeTupleGetPostingOffset) to the base address of the tuple, effectively providing direct access to the array of ItemPointer structures that represent the heap tuple identifiers sharing the same index key value. This is a fundamental accessor function for working with posting lists in PostgreSQL's B-tree deduplication system.

## Parameters / Member Variables
- posting: The IndexTuple that is a posting list tuple from which to get the posting array pointer

## Dependencies
- Functions called/Symbols referenced:
  - BTreeTupleGetPostingOffset
- Called from (representative examples):
  - _bt_dedup_start_pending
  - _bt_dedup_save_htid
  - _bt_form_posting
  - _bt_update_posting
  - btreevacuumposting
  - BTreeTupleGetPostingN
  - BTreeTupleGetHeapTID

## Notes and Other Information
- Returns an ItemPointer which can be treated as an array of ItemPointer structures
- The number of elements in the returned array can be obtained using BTreeTupleGetNPosting
- Essential for iterating through all heap TIDs in a posting list
- The returned pointer is properly aligned and points to valid ItemPointer data
- Used extensively throughout B-tree operations that need to access individual heap TIDs