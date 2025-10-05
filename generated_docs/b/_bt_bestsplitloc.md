# _bt_bestsplitloc

## Location
[src/backend/access/nbtree/nbtsplitloc.c:788-848](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsplitloc.c#L788-L848)

## Overview
Finds the optimal split point among candidate split points by selecting the one with the lowest penalty score within the current split interval.

## Definition

```c
static OffsetNumber
_bt_bestsplitloc(FindSplitData *state, int perfectpenalty,
				 bool *newitemonleft, FindSplitStrat strategy)
```
## Detailed Description
This function evaluates all candidate split points within the acceptable split interval and selects the one with the lowest penalty score. The penalty calculation varies depending on whether splitting a leaf or internal page. The function includes special handling for the "many duplicates" strategy to avoid creating succession of right half pages with unusable free space during monotonically decreasing insertions.

The function implements an optimization where it can return early if it finds a split point with the perfect penalty score, avoiding unnecessary penalty calculations for remaining candidates. It also includes logic to prevent problematic split behavior when dealing with large groups of duplicate values.

## Parameters / Member Variables
- `*state`: FindSplitData structure containing split candidates and page information
- `perfectpenalty`: The theoretical lowest possible penalty score, used for early termination optimization
- `*newitemonleft`: Output parameter indicating whether the new item will be placed on the left page after split
- `strategy`: FindSplitStrat enum indicating the splitting strategy being used (affects duplicate handling)
## Dependencies
- Functions called/Symbols referenced:
  - [_bt_split_penalty](_bt_split_penalty.md)
  - Min (macro)
  - INT_MAX
- Structures/Types referenced:
  - FindSplitData
  - FindSplitStrat
  - SplitPoint
  - SPLIT_MANY_DUPLICATES
- Called from (representative examples):
  - [_bt_findsplitloc](_bt_findsplitloc.md)

## Notes and Other Information
- This is a static function used only within nbtsplitloc.c for B-tree split optimization
- Includes special logic to handle the "many duplicates" problem where repeated splits could create unusable right half pages
- The penalty-based selection ensures optimal split points that balance page utilization and key distribution
- Returns the offset number of the first tuple that should go on the right page after split
- The perfectpenalty parameter enables performance optimization by allowing early exit from penalty calculations

## Simplified Source
```c
static OffsetNumber
_bt_bestsplitloc(FindSplitData *state, int perfectpenalty,
                 bool *newitemonleft, FindSplitStrat strategy)
{
    int bestpenalty = INT_MAX;
    int lowsplit = 0;
    int highsplit = Min(state->interval, state->nsplits);

    // Find split point with lowest penalty within the acceptable interval
    for (int i = lowsplit; i < highsplit; i++) {
        int penalty = _bt_split_penalty(state, state->splits + i);

        if (penalty < bestpenalty) {
            bestpenalty = penalty;
            lowsplit = i;
        }

        // Early exit if we achieve perfect penalty
        if (penalty <= perfectpenalty)
            break;
    }

    SplitPoint *final = &state->splits[lowsplit];

    // Special handling for "many duplicates" strategy
    // Avoid repeatedly splitting at same point during monotonically decreasing insertions
    if (strategy == SPLIT_MANY_DUPLICATES && !state->is_rightmost &&
        !final->newitemonleft && final->firstrightoff >= state->newitemoff &&
        final->firstrightoff < state->newitemoff + 9) {

        // Use 50:50 split to avoid unusable right half pages
        final = &state->splits[0];
    }

    // Return results
    *newitemonleft = final->newitemonleft;
    return final->firstrightoff;
}
```