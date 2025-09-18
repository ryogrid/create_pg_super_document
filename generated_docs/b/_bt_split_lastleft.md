# _bt_split_lastleft

## Location
src/backend/access/nbtree/nbtsplitloc.c: 1159 - 1174

## Overview
Retrieves the last IndexTuple that would remain on the left page for a given B-tree split point candidate.

## Definition
static inline IndexTuple _bt_split_lastleft(FindSplitData *state, SplitPoint *split)

## Detailed Description
This function determines and returns the IndexTuple that would be the last tuple remaining on the left page after a B-tree page split at the specified split point. This tuple is crucial for determining the new high key for the left page after the split operation.

The function handles two scenarios:
1. When the new item being inserted would be placed on the left side and the first right offset coincides with the new item's position, it returns the new item being inserted.
2. Otherwise, it retrieves the tuple immediately before the first right tuple from the original page.

This function works in conjunction with _bt_split_firstright to provide the boundary tuples needed for split penalty calculations and key truncation operations.

## Parameters / Member Variables
- `state`: Pointer to FindSplitData structure containing split operation context, including the original page, new item details, and relation information
- `split`: Pointer to SplitPoint structure specifying the candidate split point with information about where to divide the page

## Dependencies
- Functions called/Symbols referenced:
  - PageGetItemId
  - OffsetNumberPrev
  - PageGetItem
- Called from (representative examples):
  - _bt_split_penalty
  - _bt_strategy

## Notes and Other Information
- This is a static inline function for optimal performance during B-tree split operations
- The function is essential for determining the correct high key for the left page after a split
- It handles the special case where the new item being inserted affects the split boundary
- Works as a pair with _bt_split_firstright to define the split boundary tuples
- The returned IndexTuple is used in penalty calculations and key truncation algorithms