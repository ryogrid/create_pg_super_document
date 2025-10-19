# BTreeTupleGetPostingN

## Location
[src/include/access/nbtree.h:544-555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L544-L555)

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
  - [BTreeTupleGetPosting](BTreeTupleGetPosting.md)
- Called from (representative examples):
  - [_bt_bottomupdel_finish_pending](../b/_bt_bottomupdel_finish_pending.md)
  - [_bt_update_posting](../b/_bt_update_posting.md)
  - [_bt_swap_posting](../b/_bt_swap_posting.md)
  - [_bt_check_unique](../b/_bt_check_unique.md)
  - [_bt_simpledel_pass](../b/_bt_simpledel_pass.md)
  - [_bt_binsrch_posting](../b/_bt_binsrch_posting.md)
  - [_bt_readpage](../b/_bt_readpage.md)
  - [BTreeTupleGetMaxHeapTID](BTreeTupleGetMaxHeapTID.md)

## Notes and Other Information
- The caller must ensure that n is within valid bounds (0 <= n < BTreeTupleGetNPosting(posting))
- Returns a pointer to an ItemPointer structure representing the heap TID
- Commonly used in loops to iterate through all heap TIDs in a posting list
- Essential for operations that need random access to posting list elements
- Used extensively in B-tree maintenance operations and search routines

## Simplified Source

```c
static inline ItemPointer
BTreeTupleGetPostingN(IndexTuple posting, int n)
{
    // Get the Nth posting entry by adding offset to base posting array
    return BTreeTupleGetPosting(posting) + n;
}
```