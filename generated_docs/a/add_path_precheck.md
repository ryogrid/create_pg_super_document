# add_path_precheck

## Location
src/backend/optimizer/util/pathnode.c: 642 - 746

## Overview
Performs a lightweight check to determine whether a proposed path could potentially be accepted before creating the full Path structure.

## Definition


## Detailed Description
This function provides an optimization for the path creation process by performing a preliminary check before the expensive Path structure creation. It determines if a proposed path with given characteristics could possibly be accepted into the pathlist.

The function searches for existing paths with the same parameterization that would dominate the proposed path on all criteria:
- Total cost (with fuzzy comparison using STD_FUZZ_FACTOR)  
- Startup cost (if consider_startup is true)
- Pathkeys (equal or better ordering)

Key assumptions:
- Row count estimates are too expensive to compute for prechecking
- Paths with superset parameterization generate fewer rows
- Paths with different parameterizations cannot dominate each other

The function leverages the fact that pathlist is sorted by total_cost to exit early when encountering more expensive paths.

## Parameters / Member Variables
- : RelOptInfo structure containing the existing pathlist
- : Estimated startup cost for the proposed path
- : Estimated total cost for the proposed path  
- : Sort ordering for the proposed path
- : Set of required outer relations for parameterization

## Dependencies
- Functions called/Symbols referenced:
  - [compare_pathkeys](../c/compare_pathkeys.md)
  - [bms_equal](../b/bms_equal.md)
  - PATH_REQ_OUTER
  - STD_FUZZ_FACTOR (constant)
  - PATHKEYS_EQUAL, PATHKEYS_BETTER2 (enum values)
  - Cost (type)
  - PathKeysComparison (type)
- Called from (representative examples):
  - [try_nestloop_path](../t/try_nestloop_path.md)
  - [try_mergejoin_path](../t/try_mergejoin_path.md)  
  - [try_hashjoin_path](../t/try_hashjoin_path.md)
  - [add_partial_path_precheck](add_partial_path_precheck.md)

## Notes and Other Information
This function is an important performance optimization that avoids creating Path structures that would be immediately discarded. It follows the same policy as add_path regarding parameterized paths having no pathkeys. The early exit capability based on sorted pathlist can significantly reduce planning time for relations with many potential paths.