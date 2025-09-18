# BTreeTupleGetHeapTID

## Location
[src/include/access/nbtree.h:638-663](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L638-L663)

## Overview
BTreeTupleGetHeapTID retrieves the tiebreaker heap TID attribute from a B-tree index tuple, returning the first/lowest heap TID in the case of a posting list tuple.

## Definition


## Detailed Description
This function extracts the heap TID (Tuple Identifier) from a B-tree index tuple, which serves as a tiebreaker when comparing tuples with identical key values. The function handles three different tuple types:

1. **Pivot tuples**: For pivot tuples that have heap TID representation (indicated by BT_PIVOT_HEAP_TID_ATTR flag), it calculates the heap TID location at the end of the tuple structure. If the heap TID attribute was truncated, it returns NULL.

2. **Posting list tuples**: For posting tuples (which contain multiple heap TIDs), it delegates to BTreeTupleGetPosting() to get the first posting entry.

3. **Regular tuples**: For standard index tuples, it directly returns the t_tid field from the tuple structure.

The function is critical for B-tree operations that need to uniquely identify tuples, especially during comparisons and deduplication processes.

## Parameters / Member Variables
- : IndexTuple pointer to the B-tree tuple from which to extract the heap TID

## Dependencies
- Functions called/Symbols referenced:
  - [BTreeTupleIsPivot](BTreeTupleIsPivot.md)
  - [ItemPointerGetOffsetNumberNoCheck](../I/ItemPointerGetOffsetNumberNoCheck.md)  
  - BT_PIVOT_HEAP_TID_ATTR
  - IndexTupleSize
  - [BTreeTupleIsPosting](BTreeTupleIsPosting.md)
  - [BTreeTupleGetPosting](BTreeTupleGetPosting.md)
- Called from (representative examples):
  - _bt_bottomupdel_finish_pending
  - [_bt_compare](../b/_bt_compare.md)
  - [_bt_truncate](../b/_bt_truncate.md)
  - [_bt_mkscankey](../b/_bt_mkscankey.md)

## Notes and Other Information
- This is an inline function defined in nbtree.h for performance optimization
- Returns NULL for pivot tuples when the heap TID attribute has been truncated
- For posting list tuples, returns the first heap TID in the list, which serves as the minimum value for comparison purposes
- The function is essential for maintaining B-tree ordering and uniqueness constraints
- Used extensively throughout B-tree maintenance operations including deduplication, comparison, and truncation