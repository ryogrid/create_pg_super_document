# _bt_afternewitemoff

## Location
src/backend/access/nbtree/nbtsplitloc.c: 630 - 748

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
  - PageGetItemId: Get item ID from page offset
  - PageGetItem: Get item data from page and item ID
  - _bt_keep_natts_fast: Fast comparison of tuple attributes
  - BTreeTupleIsPosting: Check if tuple is a posting list
  - _bt_adjacenthtid: Check heap TID adjacency for recent insertion detection
  - OffsetNumberPrev: Get previous offset number
- Called from:
  - _bt_findsplitloc: Main split location finder for leaf page optimization

## Notes and Other Information
- Only applies to composite indexes (single key indexes return false immediately)
- Requires all tuples to be the same size to ensure fixed-width ordinal keys
- Uses heap TID adjacency as a heuristic to detect recent insertions and avoid misapplication
- Self-limiting optimization that converges on maintaining leaf fill factor over time
- Helps with "low cardinality leading column, high cardinality suffix column" index patterns
- The optimization is conservative to avoid misapplication in random insertion patterns
- Returns false for first key insertions since they dont indicate ascending patterns