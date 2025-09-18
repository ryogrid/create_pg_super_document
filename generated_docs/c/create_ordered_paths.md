# create_ordered_paths

## Location
[src/backend/optimizer/plan/planner.c:5306-5520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L5306-L5520)

## Overview
Builds a new upperrel containing paths for ORDER BY evaluation, ensuring all paths satisfy the required ordering through explicit sorting or incremental sorting optimizations.

## Definition


## Detailed Description
This function creates execution paths for ORDER BY operations by building an UPPERREL_ORDERED relation containing paths that satisfy the sort requirements. The function implements intelligent sorting strategies by first checking if input paths are already sorted according to the required sort_pathkeys, and only creating new sorted paths when necessary.

The function handles both serial and parallel execution scenarios:
- For serial paths, it considers full sorts and incremental sorts on existing paths
- For parallel execution, it generates Gather Merge paths by sorting partial paths and combining them
- It applies projection steps when the sorted path's target doesn't match the required target
- It integrates with FDW systems for distributed query processing

The optimization strategy prioritizes reusing existing sort order and applies incremental sorting when paths are partially sorted, which can significantly reduce sorting costs compared to full sorts.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and sort_pathkeys requirements
- : RelOptInfo containing source data paths to be sorted
- : PathTarget specifying the output target list the result paths must emit
- : Boolean indicating whether the target is safe for parallel execution
- : Estimated bound on number of output tuples, or -1 if no LIMIT or couldn't estimate

## Dependencies
- Functions called/Symbols referenced:
  - fetch_upper_rel
  - [pathkeys_count_contained_in](../p/pathkeys_count_contained_in.md)
  - [create_sort_path](create_sort_path.md)
  - [create_incremental_sort_path](create_incremental_sort_path.md)
  - [apply_projection_to_path](../a/apply_projection_to_path.md)
  - [create_gather_merge_path](create_gather_merge_path.md)
  - [add_path](../a/add_path.md)
- Called from:
  - [grouping_planner](../g/grouping_planner.md)

## Notes and Other Information
- Only considers sort_pathkeys, unlike generate_useful_gather_paths which looks at other pathkeys
- Preserves FDW relationship information from input to ordered relation
- For parallel execution, creates Gather Merge paths when sorting partial paths makes sense
- Respects enable_incremental_sort configuration parameter for optimization decisions
- Uses limit_tuples parameter to optimize sort operations when LIMIT is present
- The function ensures at least one path is available in the result (Assert at end)
- Does not call set_cheapest as grouping_planner handles that responsibility
- Supports extension hooks for custom path generation via create_upper_paths_hook