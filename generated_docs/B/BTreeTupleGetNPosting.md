# BTreeTupleGetNPosting

## Location
[src/include/access/nbtree.h:518-528](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L518-L528)

## Overview
BTreeTupleGetNPosting is an inline function that extracts the number of heap TIDs contained in a posting list tuple.

## Definition
static inline uint16 BTreeTupleGetNPosting(IndexTuple posting)

## Detailed Description
This function retrieves the count of heap tuple identifiers (TIDs) stored in a posting list tuple. It extracts this information from the offset number field of the tuple's t_tid, applying the BT_OFFSET_MASK to isolate the count bits from the combined value that includes both the count and the BT_IS_POSTING flag. This is essential for determining how many heap TIDs are associated with a particular index key value in PostgreSQL's B-tree deduplication system.

## Parameters / Member Variables
- posting: The IndexTuple that is a posting list tuple from which to extract the TID count

## Dependencies
- Functions called/Symbols referenced:
  - [BTreeTupleIsPosting](BTreeTupleIsPosting.md)
  - [ItemPointerGetOffsetNumberNoCheck](../I/ItemPointerGetOffsetNumberNoCheck.md)
- Constants used:
  - BT_OFFSET_MASK
- Called from (representative examples):
  - [_bt_dedup_start_pending](../b/_bt_dedup_start_pending.md)
  - [_bt_dedup_save_htid](../b/_bt_dedup_save_htid.md)
  - _bt_update_posting
  - [_bt_check_unique](../b/_bt_check_unique.md)
  - [btvacuumpage](../b/btvacuumpage.md)
  - [_bt_binsrch_posting](../b/_bt_binsrch_posting.md)
  - [BTreeTupleGetMaxHeapTID](BTreeTupleGetMaxHeapTID.md)

## Notes and Other Information
- The function includes an assertion to verify the tuple is actually a posting tuple
- Returns only the TID count portion by masking out the BT_IS_POSTING flag
- Widely used throughout the B-tree implementation for processing posting tuples
- Essential for memory allocation and iteration when working with posting lists
- The returned value is always greater than 1 for valid posting tuples