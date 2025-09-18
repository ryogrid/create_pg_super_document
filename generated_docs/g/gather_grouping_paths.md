# gather_grouping_paths

## Location
src/backend/optimizer/plan/planner.c: 7578 - 7662

## Overview
Generates optimized Gather and Gather Merge paths for grouping relations by creating both unsorted gather operations and sorted gather merge operations with intelligent sorting strategies.

## Definition


## Detailed Description
This function is specifically designed for grouped or partially grouped relations and creates parallel execution paths to collect results from worker processes. It performs several key optimizations:

1. **Pathkey trimming**: Removes ORDER BY/DISTINCT aggregate pathkeys that are no longer needed after partial aggregation
2. **Dual gather strategy**: Uses generate_useful_gather_paths for standard gather operations on existing paths
3. **Smart sorting decisions**: For each unsorted partial path, decides between full sorting vs incremental sorting based on presorted keys
4. **Gather Merge optimization**: Creates Gather Merge paths that efficiently combine sorted results from parallel workers
5. **Cost-based selection**: Only considers sorting paths that are likely beneficial (cheapest path or partially presorted paths with incremental sort enabled)

The function ensures optimal parallel result collection by balancing sorting costs against merge benefits, particularly for group-by operations where maintaining order can significantly improve performance.

## Parameters / Member Variables
- : PlannerInfo containing query planning context, group pathkeys, and other metadata
- : RelOptInfo representing the grouped or partially grouped relation for which to generate gather paths

## Dependencies
- Functions called/Symbols referenced:
  - list_copy_head
  - generate_useful_gather_paths
  - pathkeys_count_contained_in
  - create_sort_path
  - create_incremental_sort_path
  - create_gather_merge_path
  - add_path
- Called from (representative examples):
  - add_paths_to_grouping_rel
  - create_ordinary_grouping_paths
  - standard_qp_extra

## Notes and Other Information
- Should only be used with grouped or partially grouped relations due to explicit group_pathkeys references
- Passes 'true' as third argument to generate_useful_gather_paths, indicating this is for grouped relations
- Implements intelligent sorting strategy: full sort for unordered paths, incremental sort for partially ordered paths
- Considers enable_incremental_sort setting when deciding between sorting strategies
- Trims pathkeys to exclude ORDER BY/DISTINCT aggregate keys that are handled post-aggregation
- Creates Gather Merge paths with proper total_groups estimation for accurate parallel cost calculation
- Location: src/backend/optimizer/plan/planner.c:7578-7662