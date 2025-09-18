# BTreeTupleGetMaxHeapTID

## Location
[src/include/access/nbtree.h:664-684](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L664-L684)

## Overview
BTreeTupleGetMaxHeapTID retrieves the maximum heap TID attribute from a non-pivot B-tree index tuple, returning the highest TID value in posting list tuples or the only TID in regular tuples.

## Definition
static inline ItemPointer BTreeTupleGetMaxHeapTID(IndexTuple itup)

## Detailed Description
This function extracts the maximum heap TID (Tuple Identifier) from non-pivot B-tree index tuples. It is specifically designed to work with leaf-level tuples and provides complementary functionality to BTreeTupleGetHeapTID. The function handles two types of non-pivot tuples:

1. **Posting list tuples**: For tuples that contain multiple heap TIDs (posting lists), it retrieves the count of posting entries and returns the last (highest) TID in the list using BTreeTupleGetPostingN with the maximum index.

2. **Regular tuples**: For standard index tuples without posting lists, it directly returns the t_tid field from the tuple structure, which represents both the minimum and maximum TID.

The function includes an assertion to ensure it is only called with non-pivot tuples, as pivot tuples have different TID representation semantics and should not be processed by this function.

## Parameters / Member Variables
- `itup`: IndexTuple pointer to the non-pivot B-tree tuple from which to extract the maximum heap TID

## Dependencies
- Functions called/Symbols referenced:
  - [BTreeTupleIsPivot](BTreeTupleIsPivot.md) (used in assertion)
  - [BTreeTupleIsPosting](BTreeTupleIsPosting.md)
  - [BTreeTupleGetNPosting](BTreeTupleGetNPosting.md)
  - [BTreeTupleGetPostingN](BTreeTupleGetPostingN.md)
- Called from (representative examples):
  - _bt_bottomupdel_finish_pending
  - [_bt_swap_posting](../b/_bt_swap_posting.md)
  - [_bt_compare](../b/_bt_compare.md)
  - [_bt_truncate](../b/_bt_truncate.md)

## Notes and Other Information
- This is an inline function defined in nbtree.h for performance optimization
- Only works with non-pivot tuples - contains assertion to verify this precondition
- For posting list tuples, returns the last TID which represents the maximum value in the sorted posting array
- For regular tuples, the maximum and minimum heap TID are the same (the single t_tid value)
- Used primarily in B-tree operations that need to determine the range of heap TIDs covered by a tuple
- Critical for comparison operations and tuple truncation logic in B-tree maintenance
- Complements BTreeTupleGetHeapTID which returns the minimum heap TID