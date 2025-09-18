# BTreeTupleGetHeapTID

## Location
src/include/access/nbtree.h: 638 - 663

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
  - BTreeTupleIsPivot
  - ItemPointerGetOffsetNumberNoCheck  
  - BT_PIVOT_HEAP_TID_ATTR
  - IndexTupleSize
  - BTreeTupleIsPosting
  - BTreeTupleGetPosting
- Called from (representative examples):
  - _bt_bottomupdel_finish_pending
  - _bt_compare
  - _bt_truncate
  - _bt_mkscankey

## Notes and Other Information
- This is an inline function defined in nbtree.h for performance optimization
- Returns NULL for pivot tuples when the heap TID attribute has been truncated
- For posting list tuples, returns the first heap TID in the list, which serves as the minimum value for comparison purposes
- The function is essential for maintaining B-tree ordering and uniqueness constraints
- Used extensively throughout B-tree maintenance operations including deduplication, comparison, and truncation