# _bt_findinsertloc

## Location
src/backend/access/nbtree/nbtinsert.c: 815 - 1026

## Overview
Finds the exact insertion location for a tuple within a B-tree leaf page, handling page movement and space optimization.

## Definition


## Detailed Description
The  function determines the precise offset within a leaf page where a new tuple should be inserted. It handles the complex scenarios that arise when uniqueness checking has been performed and when pages need to be traversed to find the optimal insertion location.

For heapkeyspace indexes, the function may need to step right through sibling pages when uniqueness checking initially found the first page with duplicates, but the heap TID attribute requires insertion on a later page. For non-heapkeyspace indexes, it implements a probabilistic algorithm to balance insertion location optimization with performance, using a 99% probability to continue searching for better pages.

The function includes space optimization logic, attempting deletion and deduplication when the target page lacks sufficient space. It also handles the special case of overlapping posting list tuples with LP_DEAD bits set.

## Parameters / Member Variables
- : The B-tree index relation being inserted into
- : Current insertion state containing tuple, buffer, and cached search bounds
- : Indicates if uniqueness checking was performed
- : Hint that this is an UPDATE without logical change to indexed value
- : Search stack for potential page split operations
- : The heap relation associated with the index

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_check_third_page](_bt_check_third_page.md): Validates 1/3 page size restriction
  - [_bt_compare](_bt_compare.md): Compares scan key with page high key
  - [_bt_stepright](_bt_stepright.md): Moves to next sibling page
  - [_bt_delete_or_dedup_one_page](_bt_delete_or_dedup_one_page.md): Attempts space reclamation through deletion/deduplication
  - [_bt_binsrch_insert](_bt_binsrch_insert.md): Performs binary search for exact insertion offset
  - [pg_prng_uint32](../p/pg_prng_uint32.md): Generates random numbers for probabilistic page selection
- Called from (representative examples):
  - [_bt_doinsert](_bt_doinsert.md): Main insertion routine after uniqueness checking

## Notes and Other Information
- Returns OffsetNumber indicating exact insertion position within the chosen page
- May change the buffer in insertstate if stepping right to find optimal page
- Reuses cached binary search bounds from _bt_check_unique when available
- Implements different algorithms for heapkeyspace vs non-heapkeyspace indexes
- For non-heapkeyspace indexes, uses probabilistic (99% chance) right-stepping to prevent O(N^2) behavior
- Handles posting list tuple conflicts by performing deletion before final insertion
- Validates that final page choice satisfies high key constraints
- Space optimization attempts include both simple deletion and deduplication strategies