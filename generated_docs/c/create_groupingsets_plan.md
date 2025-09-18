# create_groupingsets_plan

## Location
[src/backend/optimizer/plan/createplan.c:2393-2550](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L2393-L2550)

## Overview
Creates a plan for GroupingSetsPath operations, implementing SQL GROUPING SETS, ROLLUP, and CUBE functionality by generating a main Agg plan with subsidiary Agg and Sort nodes.

## Definition
```c
static Plan *create_groupingsets_plan(PlannerInfo *root, GroupingSetsPath *best_path)
```

## Detailed Description
The `create_groupingsets_plan` function constructs a complex aggregation plan for handling advanced grouping operations like GROUPING SETS, ROLLUP, and CUBE. It creates a top-level Agg node that implements the last grouping set specified in the GroupingSetsPath, with additional grouping sets represented as subsidiary Agg and Sort nodes in a "chain" list. The function first creates a subplan, builds a grouping map to translate target list references to column positions, and then constructs the chain of subsidiary nodes for intermediate grouping operations. Each rollup in the path gets its own Agg plan with appropriate strategy (hashed, sorted, or plain) and optional Sort node if needed.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state, including processed_groupClause and groupingSets
- `best_path`: GroupingSetsPath structure containing rollups list, aggregation strategy, and cost information

## Dependencies
- Functions called/Symbols referenced:
  - [create_plan_recurse](create_plan_recurse.md)
  - [get_sortgroupclause_tle](../g/get_sortgroupclause_tle.md)
  - [remap_groupColIdx](../r/remap_groupColIdx.md)
  - [make_sort_from_groupcols](../m/make_sort_from_groupcols.md)
  - [make_agg](../m/make_agg.md)
  - [build_path_tlist](../b/build_path_tlist.md)
  - [extract_grouping_ops](../e/extract_grouping_ops.md)
  - [extract_grouping_collations](../e/extract_grouping_collations.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
- Called from (representative examples):
  - [create_plan_recurse](create_plan_recurse.md)

## Notes and Other Information
- The function is static, used only within createplan.c
- Requires that root->parse->groupingSets is not null and rollups is not empty
- Creates and stores a grouping_map in root for later use by setrefs.c to fix GroupingFunc nodes
- Subsidiary nodes in the chain don't participate directly in execution but represent required data for additional steps
- Only the topmost Agg node's costs are meaningful for EXPLAIN output
- Handles three aggregation strategies: AGG_HASHED, AGG_PLAIN, and AGG_SORTED based on rollup characteristics
- Optimizes by removing unnecessary target lists and left trees from subsidiary sort plans to reduce debug output bloat