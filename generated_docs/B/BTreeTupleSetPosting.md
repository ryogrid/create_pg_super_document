# BTreeTupleSetPosting

## Location
[src/include/access/nbtree.h:504-517](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L504-L517)

## Overview
BTreeTupleSetPosting is an inline function that configures an IndexTuple to be a posting list tuple by setting appropriate flags and storing the number of heap TIDs and posting offset information.

## Definition
static inline void BTreeTupleSetPosting(IndexTuple itup, uint16 nhtids, int postingoffset)

## Detailed Description
This function transforms a regular B-tree index tuple into a posting list tuple, which is used in PostgreSQL's B-tree implementation to store multiple heap tuple identifiers (TIDs) that share the same key value. The function sets the INDEX_ALT_TID_MASK flag to indicate this is a posting tuple, stores the number of heap TIDs in the offset number field of t_tid, and stores the posting offset (pointing to the actual posting list data) in the block number field of t_tid. This is part of PostgreSQL's deduplication mechanism for B-tree indexes.

## Parameters / Member Variables
- itup: The IndexTuple to be configured as a posting list tuple
- nhtids: Number of heap TIDs in the posting list (must be > 1)
- postingoffset: Byte offset to the posting list data (must be MAXALIGN'd and < INDEX_SIZE_MASK)

## Dependencies
- Functions called/Symbols referenced:
  - [BTreeTupleIsPivot](BTreeTupleIsPivot.md)
  - [ItemPointerSetOffsetNumber](../I/ItemPointerSetOffsetNumber.md)
  - [ItemPointerSetBlockNumber](../I/ItemPointerSetBlockNumber.md)
- Constants used:
  - BT_STATUS_OFFSET_MASK
  - INDEX_SIZE_MASK
  - INDEX_ALT_TID_MASK
  - BT_IS_POSTING
- Called from (representative examples):
  - [_bt_form_posting](../b/_bt_form_posting.md)
  - [_bt_update_posting](../b/_bt_update_posting.md)

## Notes and Other Information
- The function includes several assertions to validate input parameters
- Only non-pivot tuples can be converted to posting tuples
- The nhtids parameter is combined with BT_IS_POSTING flag when stored
- The postingoffset must be properly aligned and within size limits
- This is part of PostgreSQL's B-tree deduplication feature to reduce index bloat

## Simplified Source

```c
static inline void BTreeTupleSetPosting(IndexTuple itup, uint16 nhtids, int postingoffset) {
    // Validate parameters
    Assert(nhtids > 1);
    Assert((nhtids & BT_STATUS_OFFSET_MASK) == 0);
    Assert((size_t) postingoffset == MAXALIGN(postingoffset));
    Assert(postingoffset < INDEX_SIZE_MASK);
    Assert(!BTreeTupleIsPivot(itup));

    // Mark tuple as having alternative TID format
    itup->t_info |= INDEX_ALT_TID_MASK;

    // Store number of heap TIDs + posting flag in offset number
    ItemPointerSetOffsetNumber(&itup->t_tid, (nhtids | BT_IS_POSTING));

    // Store posting list offset in block number field
    ItemPointerSetBlockNumber(&itup->t_tid, postingoffset);
}
```