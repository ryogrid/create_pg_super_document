# build_path_tlist

## Location
[src/backend/optimizer/plan/createplan.c:826-865](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L826-L865)

## Overview
Builds a target list (list of TargetEntry nodes) from a Path's pathtarget, handling nestloop parameter replacement for parameterized paths.

## Definition
```c
static List *build_path_tlist(PlannerInfo *root, Path *path)
```

## Detailed Description
build_path_tlist constructs a proper target list for plan nodes by converting the expressions in a Path's pathtarget into TargetEntry nodes. The function handles the important case of parameterized paths where lateral references in the target list expressions need to be replaced with Param nodes representing nestloop parameters. It also preserves sortgroupref information from the pathtarget for proper handling of GROUP BY and ORDER BY references.

The function iterates through each expression in the path's pathtarget, creates TargetEntry nodes with appropriate resource numbers, and maintains the sortgroupref mapping. This is essential for converting the path-based representation used during optimization into the target list format required by plan nodes during execution.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner context and state information
- `path`: The Path node whose pathtarget expressions will be converted into a target list

## Dependencies
- Functions called/Symbols referenced:
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [makeTargetEntry](../m/makeTargetEntry.md)
- Called from (representative examples):
  - [create_scan_plan](../c/create_scan_plan.md)
  - [create_gating_plan](../c/create_gating_plan.md)
  - [create_append_plan](../c/create_append_plan.md)
  - [create_merge_append_plan](../c/create_merge_append_plan.md)
  - [create_group_result_plan](../c/create_group_result_plan.md)
  - [create_project_set_plan](../c/create_project_set_plan.md)
  - [create_unique_plan](../c/create_unique_plan.md)
  - [create_gather_plan](../c/create_gather_plan.md)
  - [create_gather_merge_plan](../c/create_gather_merge_plan.md)
  - [create_projection_plan](../c/create_projection_plan.md)
  - [create_group_plan](../c/create_group_plan.md)
  - [create_agg_plan](../c/create_agg_plan.md)
  - [create_groupingsets_plan](../c/create_groupingsets_plan.md)
  - [create_minmaxagg_plan](../c/create_minmaxagg_plan.md)
  - [create_windowagg_plan](../c/create_windowagg_plan.md)
  - [create_recursiveunion_plan](../c/create_recursiveunion_plan.md)
  - [create_nestloop_plan](../c/create_nestloop_plan.md)
  - [create_mergejoin_plan](../c/create_mergejoin_plan.md)
  - [create_hashjoin_plan](../c/create_hashjoin_plan.md)

## Notes and Other Information
- Almost equivalent to make_tlist_from_pathtarget() but includes special handling for nestloop parameter replacement
- Only applies nestloop parameter replacement when the path is parameterized (path->param_info is not NULL)
- Preserves sortgroupref information from the pathtarget to maintain proper GROUP BY and ORDER BY semantics
- Creates TargetEntry nodes with sequential resource numbers starting from 1
- Essential for converting optimizer path representations into executable plan target lists
- The function is widely used throughout plan creation for various node types requiring target list construction
- Located at src/backend/optimizer/plan/createplan.c:826-865