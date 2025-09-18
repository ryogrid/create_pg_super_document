# create_mergejoin_plan

## Location
src/backend/optimizer/plan/createplan.c: 4440 - 4746

## Overview
Creates a MergeJoin plan node from a MergePath, implementing merge joins where two pre-sorted input relations are merged together based on equality conditions.

## Definition


## Detailed Description
This function creates a MergeJoin execution plan node from a MergePath. Merge joins are efficient when both input relations are already sorted (or can be cheaply sorted) on the join columns. The function handles complex pathkey matching between outer and inner relations, creates explicit Sort nodes when necessary, and sets up the merge operation arrays needed by the executor. It also handles materialize nodes for the inner relation when mark/restore operations are needed, processes join clauses appropriately for different join types, and manages redundant pathkeys and sort ordering requirements.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and state information
- : MergePath representing the chosen merge join access path with sort requirements and merge clauses

## Dependencies
- Functions called/Symbols referenced:
  - [build_path_tlist](../b/build_path_tlist.md)
  - [create_plan_recurse](create_plan_recurse.md)
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - IS_OUTER_JOIN
  - [extract_actual_join_clauses](../e/extract_actual_join_clauses.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [get_actual_clauses](../g/get_actual_clauses.md)
  - [list_difference](../l/list_difference.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [get_switched_clauses](../g/get_switched_clauses.md)
  - [make_sort_from_pathkeys](../m/make_sort_from_pathkeys.md)
  - [label_sort_with_costsize](../l/label_sort_with_costsize.md)
  - [make_material](../m/make_material.md)
  - [copy_plan_costsize](copy_plan_costsize.md)
  - [make_mergejoin](../m/make_mergejoin.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
- Called from (representative examples):
  - [create_join_plan](create_join_plan.md)

## Notes and Other Information
- Merge joins are most efficient when input relations are already sorted on join columns
- Automatically creates Sort nodes for inputs that require sorting
- Handles complex pathkey matching including redundant and partially overlapping sort orders
- May add a Material node to the inner plan to support mark/restore operations for outer joins
- Sets up arrays of merge families, collations, strategies, and null-handling flags for the executor
- Manages both simple equality joins and complex multi-column merge conditions
- The pathkey matching logic handles cases where merge clauses may reference the same pathkey multiple times
- Located at src/backend/optimizer/plan/createplan.c:4440-4746
- Part of the JOIN METHODS section of the planner