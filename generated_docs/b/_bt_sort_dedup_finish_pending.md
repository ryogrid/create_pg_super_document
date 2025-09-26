# _bt_sort_dedup_finish_pending

## Location
[src/backend/access/nbtree/nbtsort.c:1029-1062](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L1029-L1062)

## Overview
A function that finalizes a pending posting list tuple during B-tree index construction and adds it to the index, handling both single items and multi-item posting lists.

## Definition

```c
static void
_bt_sort_dedup_finish_pending(BTWriteState *wstate, BTPageState *state,
							  BTDedupState dstate)
```
## Detailed Description
This function is responsible for completing the processing of a pending deduplication state during B-tree index building. It operates similarly to  but is specifically designed for the index building phase where it uses  to add tuples.

The function handles two distinct cases:

1. **Single Item**: When only one item is pending (), it directly adds the base tuple to the index without creating a posting list.

2. **Multiple Items**: When multiple items with the same key are pending, it creates a posting list tuple by calling  to combine the base tuple with the collected heap TIDs. It then calculates the posting list overhead for space management purposes.

After processing either case, the function resets the deduplication state by clearing all counters and state variables, preparing it for the next group of items.

The posting list overhead calculation () is important for the caller to make informed decisions about page space management and potential truncation during high key creation.

## Parameters / Member Variables
- : BTWriteState structure containing the overall state of the index building operation
- : BTPageState structure containing the state for the current page being built  
- : BTDedupState structure containing the pending items to be finalized, including the base tuple and collected heap TIDs

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_buildadd](_bt_buildadd.md)
  - [_bt_form_posting](_bt_form_posting.md)
  - IndexTupleSize
  - [BTreeTupleGetPostingOffset](../B/BTreeTupleGetPostingOffset.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [_bt_load](_bt_load.md)

## Notes and Other Information
- This function is part of PostgreSQL's B-tree index deduplication infrastructure, which helps reduce index size by combining multiple heap TIDs that point to the same key value
- The function always resets the deduplication state after processing, ensuring clean state for subsequent operations
- The truncextra calculation provides the size of the posting list portion, which is useful for space management decisions in the calling code
- Memory management is handled properly with pfree() calls for dynamically allocated posting tuples
- This function is only used during index creation/building, not during normal index operations