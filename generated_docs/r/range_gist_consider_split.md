# range_gist_consider_split

## Location
[src/backend/utils/adt/rangetypes_gist.c:1621-1703](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L1621-L1703)

## Overview
Evaluates and selects the best split candidate during the double sorting split algorithm by comparing overlap and distribution ratios.

## Definition
static void range_gist_consider_split(ConsiderSplitContext *context, RangeBound *right_lower, int min_left_count, RangeBound *left_upper, int max_left_count)

## Detailed Description
This function is a key component of the double sorting split algorithm that evaluates potential split candidates and selects the optimal one. It calculates the overlap between left and right groups and the distribution ratio to determine if a proposed split is better than the currently selected one.

The function works by:
1. **Calculating distribution**: Determines the optimal left_count to achieve the most uniform distribution of entries, respecting the min_left_count and max_left_count constraints
2. **Ratio validation**: Computes the ratio of the smaller group to total entries and rejects splits with ratios below LIMIT_RATIO
3. **Overlap measurement**: Uses either subtype_diff (if available) or count-based measures to calculate overlap between groups
4. **Selection logic**: Chooses the new split if it has lower overlap, or equal overlap but better ratio than the current best

The overlap calculation is sophisticated: when subtype_diff is available, it computes the actual distance between the upper bound of the left group and lower bound of the right group. Without subtype_diff, it uses the difference between max_left_count and min_left_count as a proxy for overlap.

## Parameters / Member Variables
- : Context structure maintaining the state of the split selection process and the currently best split
- : Lower bound of the proposed right group  
- : Minimum number of entries that must be in the left group
- : Upper bound of the proposed left group
- : Maximum number of entries that can be in the left group

## Dependencies
- Functions called/Symbols referenced:
  - [call_subtype_diff](../c/call_subtype_diff.md)
  - LIMIT_RATIO (constant)
  - [ConsiderSplitContext](../C/ConsiderSplitContext.md) (struct)
  - RangeBound (struct)
  - float4 (type)
  - Min (macro)
- Called from (representative examples):
  - [range_gist_double_sorting_split](range_gist_double_sorting_split.md)

## Notes and Other Information
- This is a static function, only accessible within the rangetypes_gist.c file
- Central to the optimization logic of the double sorting split algorithm
- Uses a two-criteria selection: minimal overlap is the primary criterion, better ratio is the secondary criterion
- The LIMIT_RATIO check ensures splits are not too unbalanced
- Maintains state in the ConsiderSplitContext to track the best split found so far
- The first parameter indicates whether this is the first split being considered
- Handles cases both with and without subtype_diff functions for different range types
- The common_left and common_right values are calculated to help with later distribution of ambiguous entries
- Negative overlap values are allowed, which can occur when groups don't actually overlap