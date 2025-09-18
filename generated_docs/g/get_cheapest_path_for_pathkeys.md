# get_cheapest_path_for_pathkeys

## Location
src/backend/optimizer/path/pathkeys.c: 618 - 663

## Overview
Finds the cheapest path (according to specified cost criterion) that satisfies given pathkeys and parameterization requirements, with optional parallel-safety constraints.

## Definition


## Detailed Description
This function is a core component of PostgreSQL's query optimizer that searches through a list of candidate paths to find the one that best matches the specified ordering requirements (pathkeys) while minimizing cost. It performs cost-based optimization by comparing paths using either startup cost or total cost as the selection criterion. The function also handles parameterized paths and can optionally restrict selection to parallel-safe paths only.

The algorithm iterates through all provided paths, filtering based on parallel-safety requirements if specified, then performs cost comparison (which is cheaper than pathkey comparison) before checking if the path's pathkeys contain the required pathkeys and if the path's outer relation requirements are satisfied.

## Parameters / Member Variables
- : List of possible paths that all generate the same relation
- : Required ordering in canonical form that the selected path must satisfy
- : Allowable outer relations for parameterized paths
- : Cost selection criterion (STARTUP_COST or TOTAL_COST)
- : When true, only considers parallel-safe paths

## Dependencies
- Functions called/Symbols referenced:
  - compare_path_costs
  - pathkeys_contained_in
  - bms_is_subset
  - PATH_REQ_OUTER
  - CostSelector (type)
- Called from (representative examples):
  - generate_orderedappend_paths
  - get_cheapest_parameterized_child_path
  - generate_mergejoin_paths
  - generate_union_paths

## Notes and Other Information
- Cost comparison is performed before pathkey comparison as an optimization, since cost comparison is computationally cheaper
- Returns NULL if no suitable path is found
- The function assumes pathkeys are in canonical form
- Part of the pathkey-based optimization infrastructure in PostgreSQL's query planner