# _bt_findinsertloc

## Location
[src/backend/access/nbtree/nbtinsert.c:815-1026](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtinsert.c#L815-L1026)

## Overview
Finds the exact insertion location for a tuple within a B-tree leaf page, handling page movement and space optimization.

## Definition

```c
static OffsetNumber
_bt_findinsertloc(Relation rel,
				  BTInsertState insertstate,
				  bool checkingunique,
				  bool indexUnchanged,
				  BTStack stack,
				  Relation heapRel)
```
## Detailed Description
The  function determines the precise offset within a leaf page where a new tuple should be inserted. It handles the complex scenarios that arise when uniqueness checking has been performed and when pages need to be traversed to find the optimal insertion location.

For heapkeyspace indexes, the function may need to step right through sibling pages when uniqueness checking initially found the first page with duplicates, but the heap TID attribute requires insertion on a later page. For non-heapkeyspace indexes, it implements a probabilistic algorithm to balance insertion location optimization with performance, using a 99% probability to continue searching for better pages.

The function includes space optimization logic, attempting deletion and deduplication when the target page lacks sufficient space. It also handles the special case of overlapping posting list tuples with LP_DEAD bits set.

## Parameters / Member Variables
- `rel`: The B-tree index relation being inserted into
- `insertstate`: Current insertion state containing tuple, buffer, and cached search bounds
- `checkingunique`: Indicates if uniqueness checking was performed
- `indexUnchanged`: Hint that this is an UPDATE without logical change to indexed value
- `stack`: Search stack for potential page split operations
- `heapRel`: The heap relation associated with the index

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

## Simplified Source

```c
static OffsetNumber _bt_findinsertloc(Relation rel, BTInsertState insertstate,
                                     bool checkingunique, bool indexUnchanged,
                                     BTStack stack, Relation heapRel) {
    BTScanInsert itup_key = insertstate->itup_key;
    Page page = BufferGetPage(insertstate->buf);
    BTPageOpaque opaque;
    OffsetNumber newitemoff;

    opaque = BTPageGetOpaque(page);

    // Check 1/3 page size restriction
    if (unlikely(insertstate->itemsz > BTMaxItemSize(page)))
        _bt_check_third_page(rel, heapRel, itup_key->heapkeyspace, page, insertstate->itup);

    if (itup_key->heapkeyspace) {
        bool uniquedup = indexUnchanged;

        // For unique indexes, may need to step right to find correct page
        if (checkingunique) {
            if (insertstate->low < insertstate->stricthigh) {
                uniquedup = true;  // Found duplicate in _bt_check_unique
            }

            // Find correct page for insertion
            for (;;) {
                // Check if tuple belongs on this page using cached bounds
                if (insertstate->bounds_valid &&
                    insertstate->low <= insertstate->stricthigh &&
                    insertstate->stricthigh <= PageGetMaxOffsetNumber(page))
                    break;

                // Check high key - if tuple fits here, stop searching
                if (P_RIGHTMOST(opaque) ||
                    _bt_compare(rel, itup_key, page, P_HIKEY) <= 0)
                    break;

                // Step right to next page
                _bt_stepright(rel, heapRel, insertstate, stack);
                page = BufferGetPage(insertstate->buf);
                opaque = BTPageGetOpaque(page);
                uniquedup = true;
            }
        }

        // Try to free space if page is full
        if (PageGetFreeSpace(page) < insertstate->itemsz) {
            _bt_delete_or_dedup_one_page(rel, heapRel, insertstate, false,
                                        checkingunique, uniquedup, indexUnchanged);
        }
    } else {
        // Non-heapkeyspace index: search for page with space
        while (PageGetFreeSpace(page) < insertstate->itemsz) {
            // Try simple deletion first
            if (P_HAS_GARBAGE(opaque)) {
                _bt_delete_or_dedup_one_page(rel, heapRel, insertstate, true,
                                            false, false, false);
                if (PageGetFreeSpace(page) >= insertstate->itemsz)
                    break;
            }

            // Check if we should stop searching
            if (insertstate->bounds_valid &&
                insertstate->low <= insertstate->stricthigh &&
                insertstate->stricthigh <= PageGetMaxOffsetNumber(page))
                break;

            // Probabilistic decision to continue or stop (99% continue)
            if (P_RIGHTMOST(opaque) ||
                _bt_compare(rel, itup_key, page, P_HIKEY) != 0 ||
                pg_prng_uint32(&pg_global_prng_state) <= (PG_UINT32_MAX / 100))
                break;

            // Step right to next page
            _bt_stepright(rel, heapRel, insertstate, stack);
            page = BufferGetPage(insertstate->buf);
            opaque = BTPageGetOpaque(page);
        }
    }

    // Find exact insertion offset within the page
    newitemoff = _bt_binsrch_insert(rel, insertstate);

    // Handle special case: overlapping posting list with LP_DEAD bit
    if (insertstate->postingoff == -1) {
        _bt_delete_or_dedup_one_page(rel, heapRel, insertstate, true,
                                    false, false, false);
        insertstate->postingoff = 0;
        newitemoff = _bt_binsrch_insert(rel, insertstate);
    }

    return newitemoff;
}
```