# _bt_split_firstright

## Location
[src/backend/access/nbtree/nbtsplitloc.c:1175-1184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsplitloc.c#L1175-L1184)

## Overview
Retrieves the first IndexTuple that would be placed on the right page for a given B-tree split point candidate.

## Definition
static inline IndexTuple _bt_split_firstright(FindSplitData *state, SplitPoint *split)

## Detailed Description
This function determines and returns the IndexTuple that would be the first tuple placed on the right page after a B-tree page split at the specified split point. This tuple serves multiple purposes in the B-tree split algorithm, including penalty calculations and determining the structure of the right page after the split.

The function handles two main scenarios:
1. When the new item being inserted should not go to the left side and the first right offset coincides with the new item's position, it returns the new item being inserted.
2. Otherwise, it retrieves the tuple at the first right offset position from the original page.

This function complements _bt_split_lastleft to provide the complete boundary information needed for evaluating split point quality and performing key truncation operations.

## Parameters / Member Variables
- `state`: Pointer to FindSplitData structure containing the split operation context, including original page data, new item information, and relation details
- `split`: Pointer to SplitPoint structure that defines the candidate split point with boundary information

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
- Called from (representative examples):
  - [_bt_split_penalty](_bt_split_penalty.md)
  - [_bt_strategy](_bt_strategy.md)

## Notes and Other Information
- This is a static inline function optimized for frequent use during B-tree split operations
- Essential for determining the starting content of the right page after a split
- Handles the special case where the new item being inserted becomes the first tuple on the right page
- Works in tandem with _bt_split_lastleft to define the complete split boundary
- The returned IndexTuple is used in penalty scoring algorithms and key truncation processes
- Critical for maintaining B-tree structural integrity during page split operations