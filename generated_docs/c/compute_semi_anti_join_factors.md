# compute_semi_anti_join_factors

## Location
src/backend/optimizer/path/costsize.c: 5007 - 5103

## Overview
Estimates how much of the inner input a SEMI, ANTI, or inner_unique join can be expected to scan, computing factors needed for cost adjustments in hash or nestloop joins where execution stops scanning inner rows upon finding a match.

## Definition


## Detailed Description
This function computes estimates for SEMI, ANTI, and inner_unique joins where the executor stops scanning inner rows as soon as it finds a match to the current outer row. The function calculates two key factors:

1. **outer_match_frac**: The fraction of outer relation rows that have any matches in the inner relation
2. **match_count**: The average number of matches for each outer row that has at least one match

The function works by:
- Computing SEMI/ANTI selectivity of join clauses using 
- Computing normal inner-join selectivity for comparison
- Calculating the average match count as: 
- Clamping results to reasonable ranges

For ANTI joins, it filters out "pushed down" clauses that won't affect match logic, while SEMI joins consider all restrictinfo clauses.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner state
- : The join relation under consideration
- : The outer relation of the join
- : The inner relation of the join  
- : JOIN_SEMI, JOIN_ANTI, or assumed inner_unique if neither
- : SpecialJoinInfo relevant to this join
- : List of join qualification clauses
- : Output parameter filled with computed factors (outer_match_frac and match_count)

## Dependencies
- Functions called/Symbols referenced:
  - clauselist_selectivity
  - init_dummy_sjinfo
  - IS_OUTER_JOIN
  - RINFO_IS_PUSHED_DOWN
  - list_free
- Called from (representative examples):
  - add_paths_to_joinrel

## Notes and Other Information
- The estimates computed are path-independent and used across all join cost estimation functions
- Handles division by zero by checking  before computing average matches
- For outer joins, temporarily builds a filtered joinquals list excluding pushed-down clauses
- The match_count is clamped to a minimum of 1.0 to ensure reasonable estimates
- Used in src/backend/optimizer/path/costsize.c:5007-5103