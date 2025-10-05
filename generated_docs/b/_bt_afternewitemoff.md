# _bt_afternewitemoff

## Location
[src/backend/access/nbtree/nbtsplitloc.c:630-748](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsplitloc.c#L630-L748)

## Overview
Determines whether a non-rightmost leaf page should apply the "split after new item" optimization for patterns of localized monotonically increasing insertions in composite indexes.

## Definition
```c
static bool _bt_afternewitemoff(FindSplitData *state, OffsetNumber maxoff, int leaffillfactor, bool *usemult)
```

## Detailed Description
This function detects insertion patterns where new items are being inserted in a localized monotonically increasing manner within composite indexes. It identifies cases where leading attribute values form local groupings and anticipates further insertions in the same grouping.

The optimization works by:

1. **Equisized tuple detection**: Verifies all tuples have the same size, suggesting fixed-width ordinal keys
2. **Size constraints**: Only applies to reasonably small tuples (up to ~2 int64 or 4 int32 attributes)
3. **Attribute comparison**: Checks that at least the first attribute matches between the new item and adjacent existing items
4. **Heap TID adjacency**: For middle insertions, verifies the previous item was recently inserted by checking heap TID adjacency

The function supports two modes:
- **Fill factor mode** (`*usemult = true`): Uses standard leaf fill factor when split point would be too far right
- **Exact split mode** (`*usemult = false`): Places the new item as the last item on the left page

This optimization helps maintain consistent page density in composite indexes with localized insertion patterns, similar to how rightmost page splits work but applied to internal groupings.

## Parameters / Member Variables
- `state`: FindSplitData structure containing split context and page information
- `maxoff`: Maximum offset number on the original page
- `leaffillfactor`: Fill factor percentage for leaf pages
- `usemult`: Output parameter indicating whether to use fill factor multiplier or exact split

## Dependencies
- Functions called/Symbols referenced:
  - IndexRelationGetNumberOfKeyAttributes: Get number of key attributes in relation
  - [PageGetItemId](../P/PageGetItemId.md): Get item ID from page offset
  - [PageGetItem](../P/PageGetItem.md): Get item data from page and item ID
  - [_bt_keep_natts_fast](_bt_keep_natts_fast.md): Fast comparison of tuple attributes
  - [BTreeTupleIsPosting](../B/BTreeTupleIsPosting.md): Check if tuple is a posting list
  - [_bt_adjacenthtid](_bt_adjacenthtid.md): Check heap TID adjacency for recent insertion detection
  - OffsetNumberPrev: Get previous offset number
- Called from:
  - [_bt_findsplitloc](_bt_findsplitloc.md): Main split location finder for leaf page optimization

## Notes and Other Information
- Only applies to composite indexes (single key indexes return false immediately)
- Requires all tuples to be the same size to ensure fixed-width ordinal keys
- Uses heap TID adjacency as a heuristic to detect recent insertions and avoid misapplication
- Self-limiting optimization that converges on maintaining leaf fill factor over time
- Helps with "low cardinality leading column, high cardinality suffix column" index patterns
- The optimization is conservative to avoid misapplication in random insertion patterns
- Returns false for first key insertions since they dont indicate ascending patterns

## Simplified Source
```c
static bool
_bt_afternewitemoff(FindSplitData *state, OffsetNumber maxoff,
                    int leaffillfactor, bool *usemult)
{
    int16 nkeyatts = IndexRelationGetNumberOfKeyAttributes(state->rel);

    // Basic eligibility checks
    if (nkeyatts == 1)  // Single key indexes not supported
        return false;
    if (state->newitemoff == P_FIRSTKEY)  // First insertion doesn't indicate pattern
        return false;

    // Verify all tuples are same size (equisized)
    if (state->newitemsz != state->minfirstrightsz)
        return false;
    if (state->newitemsz * (maxoff - 1) != state->olddataitemstotal)
        return false;

    // Reject oversized tuples (limit to ~2 int64 or 4 int32 attributes)
    Size max_tuple_size = MAXALIGN(sizeof(IndexTupleData) + sizeof(int64) * 2) +
                         sizeof(ItemIdData);
    if (state->newitemsz > max_tuple_size)
        return false;

    // Case 1: New item goes after all existing items (rightmost insertion)
    if (state->newitemoff > maxoff) {
        ItemId itemid = PageGetItemId(state->origpage, maxoff);
        IndexTuple tup = (IndexTuple) PageGetItem(state->origpage, itemid);

        // Check if leading attributes match between last tuple and new item
        int keepnatts = _bt_keep_natts_fast(state->rel, tup, state->newitem);

        if (keepnatts > 1 && keepnatts <= nkeyatts) {
            *usemult = true;  // Use fill factor approach
            return true;
        }
        return false;
    }

    // Case 2: New item goes in middle - check heap TID adjacency
    ItemId itemid = PageGetItemId(state->origpage, OffsetNumberPrev(state->newitemoff));
    IndexTuple tup = (IndexTuple) PageGetItem(state->origpage, itemid);

    // Quick checks: no posting lists, heap TIDs must be adjacent
    if (BTreeTupleIsPosting(tup) ||
        !_bt_adjacenthtid(&tup->t_tid, &state->newitem->t_tid))
        return false;

    // Check attribute matching like rightmost case
    int keepnatts = _bt_keep_natts_fast(state->rel, tup, state->newitem);

    if (keepnatts > 1 && keepnatts <= nkeyatts) {
        // Calculate position interpolation
        double interp = (double) state->newitemoff / ((double) maxoff + 1);
        double leaffillfactormult = (double) leaffillfactor / 100.0;

        // Use fill factor if split would be too far right, exact split otherwise
        *usemult = (interp > leaffillfactormult);
        return true;
    }

    return false;
}
```