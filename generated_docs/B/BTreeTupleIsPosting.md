# BTreeTupleIsPosting

## Location
[src/include/access/nbtree.h:492-503](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L492-L503)

## Overview
BTreeTupleIsPosting is a static inline function that determines whether a given B-tree index tuple is a posting list tuple, which can contain multiple heap TIDs for duplicate key values.

## Definition
```c
static inline bool
BTreeTupleIsPosting(IndexTuple itup)
```

## Detailed Description
This function identifies posting list tuples in B-tree leaf pages. Posting list tuples are an optimization that allows a single index entry to reference multiple heap tuples with the same key values, reducing index size and improving performance for tables with many duplicate key values. The function checks for the presence of alternative TID information and specifically looks for the BT_IS_POSTING flag in the offset number.

The function performs two validation checks: first ensuring the tuple has alternative TID information (INDEX_ALT_TID_MASK), and then confirming the BT_IS_POSTING flag is set in the tuple's item pointer offset number.

## Parameters / Member Variables
- `itup`: The index tuple to examine for posting list characteristics

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerGetOffsetNumberNoCheck](../I/ItemPointerGetOffsetNumberNoCheck.md)
  - INDEX_ALT_TID_MASK (bitmask constant)
  - BT_IS_POSTING (flag constant)
- Called from (representative examples):
  - [_bt_dedup_start_pending](../b/_bt_dedup_start_pending.md)
  - [_bt_check_unique](../b/_bt_check_unique.md)
  - [_bt_insertonpg](../b/_bt_insertonpg.md)
  - [_bt_binsrch_posting](../b/_bt_binsrch_posting.md)
  - [_bt_compare](../b/_bt_compare.md)
  - [BTreeTupleGetNPosting](BTreeTupleGetNPosting.md)
  - [BTreeTupleGetPostingOffset](BTreeTupleGetPostingOffset.md)

## Notes and Other Information
Posting list tuples are a key feature of PostgreSQL's B-tree deduplication optimization, introduced to handle indexes with many duplicate values more efficiently. This function is extensively used throughout B-tree operations to distinguish posting tuples from regular leaf tuples and pivot tuples, as they require different processing logic. The posting list format allows multiple heap TIDs to be stored in a single index tuple, significantly reducing index bloat for non-unique indexes with high cardinality of duplicate values.