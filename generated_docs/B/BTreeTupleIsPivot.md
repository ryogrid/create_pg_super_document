# BTreeTupleIsPivot

## Location
[src/include/access/nbtree.h:480-491](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L480-L491)

## Overview
BTreeTupleIsPivot is a static inline function that determines whether a given B-tree index tuple is a pivot tuple (used in internal nodes for navigation) rather than a leaf tuple.

## Definition
```c
static inline bool
BTreeTupleIsPivot(IndexTuple itup)
```

## Detailed Description
This function identifies pivot tuples in B-tree indexes by examining the tuple's metadata flags. Pivot tuples are used in internal (non-leaf) B-tree pages to guide searches down the tree structure. The function checks two conditions: first, whether the tuple has any alternative TID information (INDEX_ALT_TID_MASK), and second, whether the BT_IS_POSTING flag is absent from the offset number.

The function can produce false negatives (but never false positives) when used with non-heapkeyspace indexes, which is an important limitation to consider when using this function with older index formats.

## Parameters / Member Variables
- `itup`: The index tuple to examine for pivot characteristics

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerGetOffsetNumberNoCheck](../I/ItemPointerGetOffsetNumberNoCheck.md)
  - INDEX_ALT_TID_MASK (bitmask constant)
  - BT_IS_POSTING (flag constant)
- Called from (representative examples):
  - [_bt_dedup_start_pending](../b/_bt_dedup_start_pending.md)
  - [_bt_check_unique](../b/_bt_check_unique.md)
  - [_bt_search](../b/_bt_search.md)
  - [BTreeTupleSetPosting](BTreeTupleSetPosting.md)
  - BTreeTupleGetNAtts
  - [BTreeTupleGetHeapTID](BTreeTupleGetHeapTID.md)

## Notes and Other Information
This function is fundamental to B-tree tuple type identification and is used extensively throughout the B-tree implementation for different handling of pivot vs. leaf tuples. The distinction is crucial because pivot tuples have different structure and semantics compared to leaf tuples - they contain fewer attributes and serve purely for navigation rather than storing actual data. The potential for false negatives with non-heapkeyspace indexes reflects PostgreSQL's evolution in B-tree tuple formats and backward compatibility considerations.

## Simplified Source

```c
static inline bool BTreeTupleIsPivot(IndexTuple itup) {
    // Check if tuple has alternative TID info (required for pivot)
    if ((itup->t_info & INDEX_ALT_TID_MASK) == 0)
        return false;

    // Pivot tuples don't have BT_IS_POSTING flag in offset number
    if ((ItemPointerGetOffsetNumberNoCheck(&itup->t_tid) & BT_IS_POSTING) != 0)
        return false;

    return true;  // Has alt TID but no posting flag = pivot tuple
}
```