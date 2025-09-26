# BTreeTupleGetPostingOffset

## Location
[src/include/access/nbtree.h:529-536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L529-L536)

## Overview
BTreeTupleGetPostingOffset is an inline function that extracts the byte offset to the posting list data from a posting list tuple.

## Definition
static inline uint32 BTreeTupleGetPostingOffset(IndexTuple posting)

## Detailed Description
This function retrieves the byte offset that points to the actual posting list data within a posting list tuple. The offset is stored in the block number field of the tuple's t_tid and indicates where in the tuple's data the array of heap TIDs begins. This offset is essential for accessing the actual heap TID array that contains the multiple heap tuple identifiers sharing the same index key value in PostgreSQL's B-tree deduplication implementation.

## Parameters / Member Variables
- posting: The IndexTuple that is a posting list tuple from which to extract the posting offset

## Dependencies
- Functions called/Symbols referenced:
  - [BTreeTupleIsPosting](BTreeTupleIsPosting.md)
  - [ItemPointerGetBlockNumberNoCheck](../I/ItemPointerGetBlockNumberNoCheck.md)
- Called from (representative examples):
  - [_bt_dedup_start_pending](../b/_bt_dedup_start_pending.md)
  - [_bt_form_posting](../b/_bt_form_posting.md)
  - [_bt_update_posting](../b/_bt_update_posting.md)
  - [_bt_setuppostingitems](../b/_bt_setuppostingitems.md)
  - [_bt_sort_dedup_finish_pending](../b/_bt_sort_dedup_finish_pending.md)
  - [_bt_recsplitloc](../b/_bt_recsplitloc.md)
  - [BTreeTupleGetPosting](BTreeTupleGetPosting.md)

## Notes and Other Information
- The function includes an assertion to verify the tuple is actually a posting tuple
- The returned offset is guaranteed to be MAXALIGN'd and within INDEX_SIZE_MASK limits
- Used in conjunction with BTreeTupleGetNPosting to access posting list data
- Essential for memory layout calculations when working with posting tuples
- The offset points to the start of the heap TID array within the tuple's data