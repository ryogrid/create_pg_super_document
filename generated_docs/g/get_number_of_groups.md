# get_number_of_groups

## Location
src/backend/optimizer/plan/planner.c: 3698 - 3819

## Overview
Estimates the number of groups produced by grouping clauses in a query, returning 1 if not grouping.

## Definition


## Detailed Description
This function calculates the estimated number of distinct groups that will be produced by GROUP BY clauses in a query. It handles multiple scenarios including plain GROUP BY, GROUPING SETS, empty grouping sets, and aggregation without grouping. For grouping sets, it also annotates the grouping sets data with estimates for each set and rollup list to help determine if some combination could be hashed instead of sorted.

The function processes different cases:
- **GROUPING SETS**: Iterates through rollup data and estimates groups for each grouping set, accumulating totals for both regular and hash-based grouping sets
- **Plain GROUP BY**: Uses the processed group clause to estimate the number of groups
- **Empty grouping sets**: Returns the count of grouping sets (one result row per set)
- **Aggregation without grouping**: Returns 1 (single aggregated result)
- **No grouping**: Returns 1 (pass-through case)

## Parameters / Member Variables
- : PlannerInfo structure containing query planning information and statistics
- : Number of output rows from the scan/join step, used as input for group estimation
- : Grouping sets data structure containing list of grouping sets and their clauses
- : Target list containing group clause references used to extract grouping expressions

## Dependencies
- Functions called/Symbols referenced:
  - get_sortgrouplist_exprs
  - estimate_num_groups  
  - forboth (macro)
- Data structures used:
  - grouping_sets_data
  - RollupData
  - GroupingSetData
- Called from:
  - standard_qp_extra
  - create_ordinary_grouping_paths
  - create_partial_grouping_paths

## Notes and Other Information
- This is a static function within the planner module, indicating it's an internal utility for group estimation
- The function is critical for cost-based optimization decisions regarding grouping strategies
- For GROUPING SETS queries, it maintains separate estimates for different grouping approaches (sort-based vs hash-based)
- The estimates are used downstream to choose between different grouping algorithms and determine memory requirements