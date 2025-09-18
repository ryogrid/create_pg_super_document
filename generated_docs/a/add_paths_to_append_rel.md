# add_paths_to_append_rel

## Location
src/backend/optimizer/path/allpaths.c: 1302 - 1713

## Overview
Generates paths for append relations by collecting all parameterizations and orderings from child relations and creating appropriate Append and MergeAppend paths.

## Definition
```c
void add_paths_to_append_rel(PlannerInfo *root, RelOptInfo *rel, List *live_childrels)
```

## Detailed Description
This function is the core path generation engine for append relations. It systematically creates multiple types of append paths:

1. **Unparameterized Append paths** using cheapest total paths from each child
2. **Startup-optimized Append paths** using cheapest startup paths when available
3. **Partial Append paths** for parallel execution using partial paths from children
4. **Parallel-aware Append paths** mixing partial and non-partial paths for optimal parallelism
5. **Ordered Append paths** for each distinct ordering found among children
6. **Parameterized Append paths** for each distinct parameterization set found among children

The function intelligently handles parallel execution by determining the optimal number of workers based on the number of child relations and their individual parallel worker requirements. It also handles special cases like single-child append relations that can inherit ordering from their child paths.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and optimization context
- `rel`: RelOptInfo structure representing the append relation to generate paths for
- `live_childrels`: List of non-dummy child RelOptInfo structures that contribute to the append relation

## Dependencies
- Functions called/Symbols referenced:
  - accumulate_append_subpath (accumulates paths from children for append path creation)
  - get_cheapest_parallel_safe_total_inner (finds cheapest parallel-safe non-partial path)
  - PATH_REQ_OUTER (macro to extract required outer relations from path)
  - compare_pathkeys (compares two pathkey lists for equivalence)
  - create_append_path (creates AppendPath node with specified subpaths)
  - generate_orderedappend_paths (creates ordered append paths for different orderings)
  - get_cheapest_parameterized_child_path (finds cheapest path with specific parameterization)
  - add_path/add_partial_path (adds paths to relation's pathlist)
- Called from (representative examples):
  - set_append_rel_pathlist (main append relation path generation)
  - generate_partitionwise_join_paths (partitionwise join optimization)
  - create_partitionwise_grouping_paths (partitionwise grouping optimization)

## Notes and Other Information
- The function implements sophisticated parallel execution planning by calculating optimal worker counts using logarithmic scaling based on child count
- It handles both pure partial paths and mixed partial/non-partial paths for parallel append execution
- Special optimization exists for single-child append relations to inherit ordering from child paths
- The function collects all unique parameterizations and orderings from children to avoid redundant path creation
- Path validation ensures that only feasible combinations of child paths are used in append path construction
- Parallel append is enabled when both the global setting and relation's parallel safety allow it