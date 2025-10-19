# BTreeTupleGetPosting

## Location
[src/include/access/nbtree.h:537-543](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L537-L543)

## Overview
BTreeTupleGetPosting is an inline function that returns a pointer to the posting list array contained within a posting list tuple.

## Definition
static inline ItemPointer BTreeTupleGetPosting(IndexTuple posting)

## Detailed Description
This function calculates and returns a pointer to the beginning of the heap TID array within a posting list tuple. It accomplishes this by adding the posting offset (obtained from BTreeTupleGetPostingOffset) to the base address of the tuple, effectively providing direct access to the array of ItemPointer structures that represent the heap tuple identifiers sharing the same index key value. This is a fundamental accessor function for working with posting lists in PostgreSQL's B-tree deduplication system.

## Parameters / Member Variables
- posting: The IndexTuple that is a posting list tuple from which to get the posting array pointer

## Dependencies
- Functions called/Symbols referenced:
  - [BTreeTupleGetPostingOffset](BTreeTupleGetPostingOffset.md)
- Called from (representative examples):
  - [_bt_dedup_start_pending](../b/_bt_dedup_start_pending.md)
  - [_bt_dedup_save_htid](../b/_bt_dedup_save_htid.md)
  - [_bt_form_posting](../b/_bt_form_posting.md)
  - [_bt_update_posting](../b/_bt_update_posting.md)
  - [btreevacuumposting](../b/btreevacuumposting.md)
  - [BTreeTupleGetPostingN](BTreeTupleGetPostingN.md)
  - [BTreeTupleGetHeapTID](BTreeTupleGetHeapTID.md)

## Notes and Other Information
- Returns an ItemPointer which can be treated as an array of ItemPointer structures
- The number of elements in the returned array can be obtained using BTreeTupleGetNPosting
- Essential for iterating through all heap TIDs in a posting list
- The returned pointer is properly aligned and points to valid ItemPointer data
- Used extensively throughout B-tree operations that need to access individual heap TIDs

## Simplified Source

```c
static inline ItemPointer BTreeTupleGetPosting(IndexTuple posting) {
    // Calculate pointer to posting list array by adding offset to tuple base
    return (ItemPointer) ((char *) posting + BTreeTupleGetPostingOffset(posting));
}
```