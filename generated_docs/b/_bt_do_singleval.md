# _bt_do_singleval

## Location
[src/backend/access/nbtree/nbtdedup.c:782-821](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtdedup.c#L782-L821)

## Overview
Determines if all non-pivot tuples on a page are duplicates of the same value to decide whether deduplication's "single value" strategy should be applied.

## Definition
```c
static bool _bt_do_singleval(Relation rel, Page page, BTDedupState state, OffsetNumber minoff, IndexTuple newitem)
```

## Detailed Description
This function implements a key decision point in B-tree deduplication by determining whether a page contains only duplicates of a single value. When this condition is met, PostgreSQL applies a special "single value" strategy that coordinates with the page splitting logic in nbtsplitloc.c.

The strategy's primary goal is to ensure that when a page containing only duplicate values eventually splits, it will end up BTREE_SINGLEVAL_FILLFACTOR% full, maintaining the same behavior as if deduplication were disabled. This helps the split logic find useful split points as more duplicates are inserted.

The function performs a simple but effective test: it compares the new item being inserted against both the first tuple (at minoff) and the last tuple on the page using _bt_keep_natts_fast(). If both comparisons indicate that more than the number of key attributes are needed to distinguish the tuples, then all tuples on the page are considered duplicates of the same value.

The implementation anticipates that affected workloads will require several deduplication passes before a page finally splits. Early passes handle regular tuples, later passes encounter posting list tuples from previous deduplication, and the final passes deliberately leave some tuples untouched to achieve the target fill factor.

## Parameters / Member Variables
- `rel`: The B-tree index relation being processed
- `page`: The B-tree page to examine for single-value duplicates
- `state`: The deduplication state (used for context, not modified in this function)
- `minoff`: The minimum offset number to consider on the page
- `newitem`: The new index tuple being inserted, used as the comparison baseline

## Dependencies
- Functions called/Symbols referenced:
  - IndexRelationGetNumberOfKeyAttributes
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [_bt_keep_natts_fast](_bt_keep_natts_fast.md)
- Called from:
  - [_bt_dedup_pass](_bt_dedup_pass.md)

## Notes and Other Information
- This is a static function within the nbtdedup.c module, part of PostgreSQL's B-tree deduplication system
- The function returns true if the single value strategy should be applied, false otherwise
- The algorithm is efficient, requiring only two tuple comparisons regardless of page size
- Works in coordination with nbtsplitloc.c's single value strategy for page splitting
- Multiple deduplication passes may be needed before a page splits, but each pass after the first is relatively inexpensive
- Located at src/backend/access/nbtree/nbtdedup.c:782-821

## Simplified Source

```c
static bool _bt_do_singleval(Relation rel, Page page, BTDedupState state,
                            OffsetNumber minoff, IndexTuple newitem) {
    int nkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);

    // Check if newitem is duplicate of first tuple on page
    ItemId itemid = PageGetItemId(page, minoff);
    IndexTuple itup = (IndexTuple) PageGetItem(page, itemid);

    if (_bt_keep_natts_fast(rel, newitem, itup) > nkeyatts) {
        // Also check against last tuple on page
        itemid = PageGetItemId(page, PageGetMaxOffsetNumber(page));
        itup = (IndexTuple) PageGetItem(page, itemid);

        if (_bt_keep_natts_fast(rel, newitem, itup) > nkeyatts) {
            return true; // All tuples are duplicates - use single value strategy
        }
    }

    return false; // Not all duplicates - use normal deduplication
}
```