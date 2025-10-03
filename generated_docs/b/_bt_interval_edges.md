# _bt_interval_edges

## Location
[src/backend/access/nbtree/nbtsplitloc.c:1052-1130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsplitloc.c#L1052-L1130)

## Overview
Locates the leftmost and rightmost split points within the current split interval to determine the boundary splits for strategy evaluation.

## Definition

```c
static void
_bt_interval_edges(FindSplitData *state, SplitPoint **leftinterval,
				   SplitPoint **rightinterval)
```
## Detailed Description
This function identifies the extreme split points (leftmost and rightmost) within the current split interval. The split interval represents the acceptable range of split candidates, and this function finds the boundary splits that will be used for further analysis in strategy selection. The function iterates backwards through the splits array (since delta distance makes extreme splits appear at the end) and identifies splits based on their firstrightoff values relative to the delta-optimal split.

The function handles special cases where the new item becomes either the first-right or last-left tuple, ensuring proper identification of interval boundaries even when split points have the same firstrightoff value but different new item placement.

## Parameters / Member Variables
- `*state`: FindSplitData structure containing split candidates and interval information
- `**leftinterval`: Output parameter pointing to the leftmost split point in the current interval
- `**rightinterval`: Output parameter pointing to the rightmost split point in the current interval
## Dependencies
- Functions called/Symbols referenced:
  - Min (macro)
  - Assert (macro)
- Structures/Types referenced:
  - FindSplitData
  - SplitPoint
- Called from (representative examples):
  - [_bt_strategy](_bt_strategy.md)

## Notes and Other Information
- This is a static function used only within nbtsplitloc.c for split interval boundary determination
- Iterates backwards through the splits array because delta distance typically places extreme splits at higher indices
- Handles edge cases where leftinterval and rightinterval may point to the same split if there's only one split in the interval
- The function distinguishes between splits with the same firstrightoff based on whether the new item becomes first-right or last-left
- Critical for strategy selection as it provides the boundary splits needed to evaluate the entire range of acceptable split points
- Uses assertions to ensure both left and right interval pointers are properly set before returning