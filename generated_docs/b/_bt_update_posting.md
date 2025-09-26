# _bt_update_posting

## Location
[src/backend/access/nbtree/nbtdedup.c:924-1021](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtdedup.c#L924-L1021)

## Overview
Generates a replacement tuple by updating a posting list tuple to remove TIDs that need to be deleted, used by both VACUUM and index deletion operations.

## Definition
```c
void _bt_update_posting(BTVacuumPosting vacposting)
```

## Detailed Description
This function creates an updated version of a posting list tuple by removing specified TIDs while preserving the remaining ones. It's a crucial component in PostgreSQL's index maintenance operations, used by both VACUUM and index deletion to efficiently update posting list tuples without having to delete and re-insert entire tuples.

The function operates by:
1. Calculating the new size needed for the updated tuple (original count minus deleted TIDs)
2. Determining whether the result should be a posting list tuple (nhtids > 1) or a standard non-pivot tuple (nhtids == 1)
3. Allocating memory for the new tuple and copying the key data from the original
4. Selectively copying TIDs from the original posting list, skipping those marked for deletion
5. Properly formatting the result as either a posting list or standard tuple

The implementation is optimized to avoid unnecessary memory allocations by directly building the result tuple rather than using _bt_form_posting() which would require an intermediate htids workspace.

The function handles the transition between posting list tuples and standard tuples seamlessly - if only one TID remains after deletion, it creates a standard non-pivot tuple rather than a posting list with a single entry.

## Parameters / Member Variables
- `vacposting`: A BTVacuumPosting structure containing:
  - `itup`: The original posting list tuple to be updated
  - `deletetids`: Array of posting list indexes marking TIDs to be deleted
  - `ndeletedtids`: Number of TIDs to be deleted

## Dependencies
- Functions called/Symbols referenced:
  - BTreeTupleGetNPosting
  - _bt_posting_valid
  - BTreeTupleGetPostingOffset
  - palloc0
  - BTreeTupleSetPosting
  - BTreeTupleGetPosting
  - BTreeTupleGetPostingN
  - ItemPointerIsValid
- Called from:
  - _bt_delitems_update
  - btree_xlog_updates

## Notes and Other Information
- This is a non-static function in the nbtdedup.c module, available to other B-tree components
- The function modifies the vacposting argument to point to the newly created updated tuple
- Memory allocation uses palloc0() to match the behavior of index_form_tuple()
- The size calculation deliberately matches the logic in _bt_form_posting() for consistency
- Handles both cases: updating to a new posting list tuple or converting to a standard tuple
- Includes comprehensive assertions to validate the correctness of the update operation
- Used in critical index maintenance operations including VACUUM and WAL recovery
- The original tuple is not modified; a completely new tuple is created
- Located at src/backend/access/nbtree/nbtdedup.c:924-1021