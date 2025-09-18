# _bt_bestsplitloc

## Location
src/backend/access/nbtree/nbtsplitloc.c: 788 - 848

## Overview
Finds the optimal split point among candidate split points by selecting the one with the lowest penalty score within the current split interval.

## Definition


## Detailed Description
This function evaluates all candidate split points within the acceptable split interval and selects the one with the lowest penalty score. The penalty calculation varies depending on whether splitting a leaf or internal page. The function includes special handling for the "many duplicates" strategy to avoid creating succession of right half pages with unusable free space during monotonically decreasing insertions.

The function implements an optimization where it can return early if it finds a split point with the perfect penalty score, avoiding unnecessary penalty calculations for remaining candidates. It also includes logic to prevent problematic split behavior when dealing with large groups of duplicate values.

## Parameters / Member Variables
- : FindSplitData structure containing split candidates and page information
- : The theoretical lowest possible penalty score, used for early termination optimization
- : Output parameter indicating whether the new item will be placed on the left page after split
- : FindSplitStrat enum indicating the splitting strategy being used (affects duplicate handling)

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