# _bt_swap_posting

## Location
[src/backend/access/nbtree/nbtdedup.c:1022-1077](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtdedup.c#L1022-L1077)

## Overview
Prepares for a posting list split by swapping heap TID in newitem with heap TID from original posting list at a specified offset, returning a new posting list tuple.

## Definition

```c
IndexTuple
_bt_swap_posting(IndexTuple newitem, IndexTuple oposting, int postingoff)
```
## Detailed Description
This function is a core component of PostgreSQL's B-tree posting list split mechanism. It performs a TID swap operation where it takes the heap TID from the original posting list at the specified offset and replaces it with the TID from the new item being inserted. The function creates a new posting list tuple that maintains the same size as the original but with modified TID arrangements.

The function handles the complex task of reorganizing posting lists during B-tree splits by:
1. Creating a copy of the original posting list
2. Shifting TIDs in the posting list to make room for the new TID
3. Inserting the new item's TID at the specified position
4. Copying the original posting list's maximum TID into the new item

The design accounts for potential representational differences between tuples that are logically equal but may have different physical representations (e.g., due to TOAST compression states).

## Parameters / Member Variables
- : The new IndexTuple being inserted - modified in place to receive the max TID from oposting
- : The original posting list tuple that will be split
- : The offset position in the posting list where the split should occur (0 < postingoff < nhtids)

## Dependencies
- Functions called/Symbols referenced:
  - [BTreeTupleGetNPosting](../B/BTreeTupleGetNPosting.md)
  - [_bt_posting_valid](_bt_posting_valid.md)
  - [CopyIndexTuple](../C/CopyIndexTuple.md)
  - [BTreeTupleGetPostingN](../B/BTreeTupleGetPostingN.md)
  - [BTreeTupleIsPivot](../B/BTreeTupleIsPivot.md)
  - [BTreeTupleIsPosting](../B/BTreeTupleIsPosting.md)
  - [ItemPointerCopy](../I/ItemPointerCopy.md)
  - [BTreeTupleGetMaxHeapTID](../B/BTreeTupleGetMaxHeapTID.md)
  - [ItemPointerCompare](../I/ItemPointerCompare.md)
  - [BTreeTupleGetHeapTID](../B/BTreeTupleGetHeapTID.md)
- Called from (representative examples):
  - [_bt_insertonpg](_bt_insertonpg.md)
  - [btree_xlog_insert](btree_xlog_insert.md)
  - [btree_xlog_split](btree_xlog_split.md)

## Notes and Other Information
- The function includes critical error checking for corruption cases where postingoff is out of valid range
- Returns a palloc'd tuple in the caller's context that is guaranteed to be the same size as the original
- The caller receives a modified newitem that contains the maximum TID from the original posting list
- This operation is performed within critical sections during B-tree page modifications
- The design supports future enhancements like page-level prefix compression
- Detailed algorithm and design rationale can be found in nbtree/README documentation