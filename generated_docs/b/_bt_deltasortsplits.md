# _bt_deltasortsplits

## Location
src/backend/access/nbtree/nbtsplitloc.c: 566 - 593

## Overview
Calculates space utilization deltas for all candidate split points based on the current fill factor and sorts the splits by these delta values to prioritize balanced splits.

## Definition
```c
static void _bt_deltasortsplits(FindSplitData *state, double fillfactormult, bool usemult)
```

## Detailed Description
This function assigns delta values to each candidate split point that represent how well-balanced the space utilization would be after the split. The delta calculation considers:

1. **Fill factor multiplier**: When `usemult` is true, applies a weighted calculation based on `fillfactormult` to favor splits that achieve the desired fill factor on the left page
2. **Simple balance**: When `usemult` is false, calculates the absolute difference between left and right free space

The deltas are computed as the absolute value of the difference between left and right free space (potentially weighted by the fill factor). Lower delta values indicate more balanced splits. After calculating deltas for all candidates, the array is sorted using `_bt_splitcmp` to order splits from most balanced (lowest delta) to least balanced.

This sorting enables subsequent functions to efficiently examine the best split candidates first when applying different split strategies.

## Parameters / Member Variables
- `state`: FindSplitData structure containing the array of candidate split points
- `fillfactormult`: Fill factor multiplier (between 0.0 and 1.0) for weighted calculations
- `usemult`: Whether to apply fill factor weighting or use simple balance calculation

## Dependencies
- Functions called/Symbols referenced:
  - qsort: Standard C library sort function
  - _bt_splitcmp: Comparison function for sorting SplitPoint structures
- Called from:
  - _bt_findsplitloc: Called twice - once for initial sorting and potentially again for single value strategy

## Notes and Other Information
- The delta calculation favors splits that leave the left page at the desired fill factor when `usemult` is true
- All delta values are stored as absolute values, ensuring the sort order prioritizes balance regardless of which side has more free space
- The sort is stable and deterministic, using `_bt_splitcmp` as the comparison function
- This function is crucial for the multi-strategy approach in split point selection, allowing different strategies to work with the same sorted candidate list
- Fill factor multipliers typically come from relation-specific settings or hardcoded constants for different page types