# statext_mcv_clauselist_selectivity

## Location
src/backend/statistics/extended_stats.c: 1696 - 1983

## Overview
Estimates clause selectivity using the best multi-column MCV (Most Common Values) statistics through a greedy algorithm that iteratively applies available statistics to maximize coverage.

## Definition


## Detailed Description
This function implements a sophisticated selectivity estimation algorithm using extended multi-column MCV statistics. It employs a greedy approach, iteratively selecting the best statistics object that covers the most remaining clauses and applying it to estimate their combined selectivity. The function handles both AND-ed and OR-ed clause lists differently: for AND clauses, it multiplies selectivities together, while for OR clauses it uses the inclusion-exclusion principle to account for overlaps. The algorithm combines simple selectivity (assuming column independence), MCV selectivity (from actual statistics), base selectivity, and total selectivity using mcv_combine_selectivities to produce accurate estimates that leverage both traditional and extended statistics.

## Parameters / Member Variables
- : PlannerInfo structure containing planning context and optimizer state
- : List of restriction clauses to estimate selectivity for
- : Variable relation ID (0 if this is a join relation)
- : Type of join if this is a join selectivity estimation
- : Special join information for outer joins
- : RelOptInfo structure containing relation information and statistics
- : Input/output bitmap tracking which clauses have been estimated (0-based indexes)
- : Boolean flag indicating whether clauses are OR-ed (true) or AND-ed (false)

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - has_stats_of_kind
  - bms_is_member
  - statext_is_compatible_clause
  - choose_best_statistics
  - bms_is_subset
  - stat_covers_expressions
  - bms_membership
  - bms_add_member
  - bms_free
  - list_free
  - statext_mcv_load
  - clause_selectivity_ext
  - clauselist_selectivity_ext
  - mcv_clause_selectivity_or
  - mcv_clauselist_selectivity
  - mcv_combine_selectivities
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - statext_clauselist_selectivity

## Notes and Other Information
The function uses a two-phase approach: first, it preprocesses clauses to extract attribute numbers and expressions, then it iteratively applies the best available statistics. For OR clauses, it implements the complex inclusion-exclusion formula P(A OR B) = P(A) + P(B) - P(A AND B) iteratively. The algorithm prioritizes simple single-column clauses by using traditional selectivity estimates for them while leveraging multi-column statistics for complex clauses. The greedy selection ensures that statistics with the most coverage are applied first, maximizing the benefit of extended statistics while falling back to traditional methods for uncovered clauses.