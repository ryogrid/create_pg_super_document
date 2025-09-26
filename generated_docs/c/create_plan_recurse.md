# create_plan_recurse

## Location
[src/backend/optimizer/plan/createplan.c:389-559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L389-L559)

## Overview
The recursive core function of create_plan that converts path nodes into corresponding plan nodes through a comprehensive switch statement handling all PostgreSQL path types.

## Definition
```c
static Plan *create_plan_recurse(PlannerInfo *root, Path *best_path, int flags)
```

## Detailed Description
create_plan_recurse serves as the recursive engine that transforms the optimizer's path-based representation into executable plan nodes. It implements a large switch statement that handles all possible path types in PostgreSQL, from basic scan operations to complex operations like joins, aggregates, and window functions. Each case delegates to a specialized plan creation function appropriate for that path type.

The function includes stack depth checking to prevent overflow from overly complex plans and maintains the planner context through the PlannerInfo structure. The flags parameter controls various aspects of target list generation and processing during plan creation.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context for the current query level
- `best_path`: The path node to be converted into a plan node, representing the chosen execution strategy
- `flags`: Control flags for plan creation behavior (e.g., CP_EXACT_TLIST, CP_IGNORE_TLIST)

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](check_stack_depth.md)
  - [create_scan_plan](create_scan_plan.md)
  - [create_join_plan](create_join_plan.md)
  - [create_append_plan](create_append_plan.md)
  - [create_merge_append_plan](create_merge_append_plan.md)
  - [create_projection_plan](create_projection_plan.md)
  - [create_minmaxagg_plan](create_minmaxagg_plan.md)
  - [create_group_result_plan](create_group_result_plan.md)
  - [create_project_set_plan](create_project_set_plan.md)
  - [create_material_plan](create_material_plan.md)
  - [create_memoize_plan](create_memoize_plan.md)
  - [create_unique_plan](create_unique_plan.md)
  - [create_upper_unique_plan](create_upper_unique_plan.md)
  - [create_gather_plan](create_gather_plan.md)
  - [create_gather_merge_plan](create_gather_merge_plan.md)
  - [create_sort_plan](create_sort_plan.md)
  - [create_incrementalsort_plan](create_incrementalsort_plan.md)
  - [create_group_plan](create_group_plan.md)
  - [create_agg_plan](create_agg_plan.md)
  - [create_groupingsets_plan](create_groupingsets_plan.md)
  - [create_windowagg_plan](create_windowagg_plan.md)
  - [create_setop_plan](create_setop_plan.md)
  - [create_recursiveunion_plan](create_recursiveunion_plan.md)
  - [create_lockrows_plan](create_lockrows_plan.md)
  - [create_modifytable_plan](create_modifytable_plan.md)
  - [create_limit_plan](create_limit_plan.md)
- Called from (representative examples):
  - [create_plan](create_plan.md)
  - [create_append_plan](create_append_plan.md)
  - [create_merge_append_plan](create_merge_append_plan.md)
  - [create_material_plan](create_material_plan.md)
  - [create_unique_plan](create_unique_plan.md)
  - [create_sort_plan](create_sort_plan.md)
  - [create_nestloop_plan](create_nestloop_plan.md)
  - [create_mergejoin_plan](create_mergejoin_plan.md)
  - [create_hashjoin_plan](create_hashjoin_plan.md)

## Notes and Other Information
- Handles all PostgreSQL path types including scans, joins, sorts, aggregates, window functions, and modify operations
- Includes stack depth protection against overly complex plans
- Uses specialized create functions for each path type to maintain modularity
- For T_Result paths, performs additional type checking to handle ProjectionPath, MinMaxAggPath, GroupResultPath, and simple RTE_RESULT cases
- For T_Unique paths, distinguishes between UpperUniquePath and UniquePath
- For T_Agg paths, handles both regular AggPath and GroupingSetsPath
- Located at src/backend/optimizer/plan/createplan.c:389-559