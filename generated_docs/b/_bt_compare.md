# _bt_compare

## Location
src/backend/access/nbtree/nbtsearch.c: 682 - 875

## Overview
Compares an insertion-type scan key to a tuple on a B-tree page, returning the comparison result used for B-tree traversal and positioning operations.

## Definition
```c
int32 _bt_compare(Relation rel, BTScanInsert key, Page page, OffsetNumber offnum)
```

## Detailed Description
This is a core B-tree comparison function that compares a scan key against a tuple at a specific offset on a page. It implements the comparison logic fundamental to B-tree operations including search, insertion, and uniqueness checking. The function handles complex scenarios including NULL values, truncated tuples, posting lists, and the special case of minus infinity for the first data key on internal pages.

The function returns standard comparison results: <0 if scankey < tuple, 0 if equal, >0 if scankey > tuple. It properly handles the Lehman and Yao convention where the first down-link pointer on internal pages is treated as minus infinity, enabling proper B-tree navigation.

## Parameters / Member Variables
- `rel`: The index relation being searched
- `key`: BTScanInsert structure containing the search key and associated metadata
- `page`: The B-tree page containing the target tuple
- `offnum`: Offset number of the tuple to compare against

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetDescr
  - BTPageGetOpaque
  - [_bt_check_natts](_bt_check_natts.md)
  - IndexRelationGetNumberOfKeyAttributes
  - P_ISLEAF
  - P_FIRSTDATAKEY
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - BTreeTupleGetNAtts
  - [BTreeTupleIsPosting](../B/BTreeTupleIsPosting.md)
  - [index_getattr](../i/index_getattr.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [BTreeTupleGetHeapTID](../B/BTreeTupleGetHeapTID.md)
  - [ItemPointerCompare](../I/ItemPointerCompare.md)
  - [BTreeTupleGetMaxHeapTID](../B/BTreeTupleGetMaxHeapTID.md)
- Called from:
  - [_bt_search_insert](_bt_search_insert.md)
  - [_bt_check_unique](_bt_check_unique.md)
  - [_bt_findinsertloc](_bt_findinsertloc.md)
  - [_bt_moveright](_bt_moveright.md)
  - [_bt_binsrch](_bt_binsrch.md)
  - [_bt_binsrch_insert](_bt_binsrch_insert.md)

## Notes and Other Information
- Handles NULL values according to NULLS FIRST/LAST ordering specifications
- Implements special minus infinity logic for first data keys on internal pages
- Supports both forward and backward scan directions
- Properly handles truncated pivot tuples in B-tree internal nodes
- Works with posting list tuples by comparing against the TID range
- Critical for maintaining B-tree structural integrity and search correctness
- The comparison logic must account for different data types and collations
- Returns consistent results essential for B-tree balance and search efficiency