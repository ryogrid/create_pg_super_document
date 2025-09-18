# get_cheapest_fractional_path_for_pathkeys

## Location
src/backend/optimizer/path/pathkeys.c: 664 - 696

## Overview
Finds the cheapest path for retrieving a specified fraction of all tuples that satisfies given pathkeys and parameterization requirements.

## Definition


## Detailed Description
This function is a specialized variant of path selection that optimizes for scenarios where only a fraction of the total result set will be retrieved. It is particularly useful for queries with LIMIT clauses or other operations that terminate early. The function uses fractional path cost comparison instead of total cost comparison, which takes into account that startup costs become more significant when only retrieving partial results.

Like its counterpart get_cheapest_path_for_pathkeys, it iterates through candidate paths, prioritizing cost comparison over pathkey comparison for efficiency, then validates that the path satisfies the required ordering (pathkeys) and parameterization constraints.

## Parameters / Member Variables
- : List of possible paths that all generate the same relation
- : Required ordering in canonical form that the selected path must satisfy
- : Allowable outer relations for parameterized paths  
- : The fraction of total tuples expected to be retrieved (between 0.0 and 1.0)

## Dependencies
- Functions called/Symbols referenced:
  - compare_fractional_path_costs
  - pathkeys_contained_in
  - bms_is_subset
  - PATH_REQ_OUTER
- Called from (representative examples):
  - generate_orderedappend_paths
  - build_minmax_path

## Notes and Other Information
- The fraction parameter is interpreted by compare_fractional_path_costs() to weight startup vs. total costs appropriately
- Returns NULL if no suitable path is found
- Optimized for queries that will terminate early (e.g., with LIMIT clauses)
- Cost comparison is performed before pathkey comparison as an optimization
- Part of PostgreSQL's fractional path cost optimization framework for partial result retrieval