# build_setop_child_paths

## Location
src/backend/optimizer/prep/prepunion.c: 504 - 695

## Overview
Builds execution paths for a set operation child relation, creating both sorted and unsorted subquery scan paths as needed for optimal set operation processing.

## Definition


## Detailed Description
 is responsible for generating all necessary execution paths for a subquery that participates in a set operation (UNION, INTERSECT, EXCEPT). This function bridges the gap between the subquery's internal paths and the paths needed by the outer set operation.

The function performs several key operations:
- Establishes equivalence relationships when sorted paths are needed for the set operation
- Sets size estimates for the relation to enable proper costing
- Configures parallel processing capabilities based on the subquery's final relation
- Creates SubqueryScan paths for each viable path from the subquery's final relation
- Handles both sorted and unsorted paths, adding sorting when beneficial for the set operation
- Supports both regular Sort and Incremental Sort operations when paths are partially pre-sorted
- Generates partial (parallel) paths when parallel processing is feasible
- Estimates the number of distinct groups in the output when requested

The function is particularly intelligent about sorting: it always includes the cheapest unsorted path for set operations that don't require sorted input, but also creates sorted paths when they would benefit operations like MergeAppend in UNIONs.

## Parameters / Member Variables
- : PlannerInfo containing the overall query planning context
- : RelOptInfo representing the set operation child relation (must be RTE_SUBQUERY)
- : Boolean indicating whether the target list requires no type conversions
- : Target list for the child relation
- : List of pathkeys that would be beneficial for sorted paths, or NIL if sorting is not needed
- : Output parameter for estimated number of distinct groups, or NULL if not needed

## Dependencies
- Functions called/Symbols referenced:
  - add_setop_child_rel_equivalences
  - set_subquery_size_estimates
  - fetch_upper_rel
  - convert_subquery_pathkeys
  - make_tlist_from_pathtarget
  - create_subqueryscan_path
  - pathkeys_count_contained_in
  - create_sort_path
  - create_incremental_sort_path
  - add_path
  - add_partial_path
  - postprocess_setop_rel
  - estimate_num_groups
  - get_tlist_exprs
  - bms_is_empty
- Called from (representative examples):
  - generate_recursion_path (src/backend/optimizer/prep/prepunion.c:419, 431)
  - generate_union_paths (src/backend/optimizer/prep/prepunion.c:772)
  - generate_nonunion_paths (src/backend/optimizer/prep/prepunion.c:1058, 1071)

## Notes and Other Information
- This is a static function, internal to the prepunion.c module
- The function only operates on RTE_SUBQUERY relations, as verified by assertion
- The function intelligently chooses between regular Sort and Incremental Sort based on how many keys are already pre-sorted
- When estimating distinct groups, the function uses special logic to handle setop subqueries with "varno 0" Vars that could confuse standard estimation
- Parallel processing is only considered when the relation has no lateral references (bms_is_empty(rel->lateral_relids))
- The function always includes the cheapest path regardless of whether sorting is needed, ensuring efficient execution for operations that don't require sorted input
- Size estimation must be completed before generating paths to ensure accurate costing by cost_subqueryscan
- The group estimation logic accounts for whether the subquery already performed grouping/aggregation, using row count directly in such cases rather than statistical estimation