# set_cheapest

## Location
src/backend/optimizer/util/pathnode.c: 242 - 419

## Overview
Analyzes all paths for a relation and identifies the minimum-cost paths for startup cost and total cost, storing them in the relation's cheapest-path fields.

## Definition


## Detailed Description
This function is a critical component of PostgreSQL's cost-based optimizer that processes all paths for a given relation and identifies the most cost-effective options. It maintains several categories of cheapest paths:

1. **cheapest_startup_path**: The unparameterized path with lowest startup cost
2. **cheapest_total_path**: The unparameterized path with lowest total cost (or best parameterized path if no unparameterized paths exist)  
3. **cheapest_parameterized_paths**: A list of all surviving parameterized paths plus the cheapest unparameterized path

For parameterized paths, the function uses a sophisticated comparison that considers both cost and the degree of parameterization (fewer required outer relations is better). When paths have identical costs, it prefers paths with better sort orderings using pathkey comparison.

## Parameters / Member Variables
- : RelOptInfo structure containing the pathlist to analyze and fields to update with cheapest paths

## Dependencies
- Functions called/Symbols referenced:
  - bms_subset_compare
  - PATH_REQ_OUTER
  - compare_path_costs
  - compare_pathkeys
  - lcons
  - BMS_EQUAL, BMS_SUBSET1, BMS_SUBSET2, BMS_DIFFERENT (enum values)
  - STARTUP_COST, TOTAL_COST (enum values)
  - PATHKEYS_BETTER2 (enum value)
- Called from (representative examples):
  - set_rel_pathlist
  - set_dummy_rel_pathlist
  - standard_join_search
  - generate_partitionwise_join_paths
  - query_planner
  - subquery_planner
  - create_grouping_paths
  - create_window_paths
  - create_distinct_paths

## Notes and Other Information
This function is typically called after all paths for a relation have been constructed and added via add_path(). It ensures that the optimizer has easy access to the most cost-effective execution options without having to search through the entire pathlist repeatedly. The function handles both unparameterized and parameterized paths, with parameterized paths requiring outer relation values to execute.