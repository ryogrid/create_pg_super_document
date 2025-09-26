# remap_groupColIdx

## Location
[src/backend/optimizer/plan/createplan.c:2355-2392](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L2355-L2392)

## Overview
Maps target list entry references in a group clause to actual column positions in the input tuple using the planner's grouping map.

## Definition
```c
static AttrNumber *remap_groupColIdx(PlannerInfo *root, List *groupClause)
```

## Detailed Description
The `remap_groupColIdx` function transforms the grouping column references from a groupClause into actual column indices that can be used during plan execution. It uses the `root->grouping_map` which maps target list entry sort group references (`tleSortGroupRef`) to actual column positions in the input tuple. This remapping is essential for grouping sets operations where the same grouping columns may need to be referenced differently depending on the specific grouping set being processed.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the grouping_map that maps tleSortGroupRef to column positions
- `groupClause`: List of SortGroupClause structures representing the grouping columns

## Dependencies
- Functions called/Symbols referenced:
  - [SortGroupClause](../S/SortGroupClause.md) (struct type)
  - [palloc0](../p/palloc0.md) (memory allocation)
  - [list_length](../l/list_length.md) (list utility)
  - lfirst (list iteration)
- Called from (representative examples):
  - [create_groupingsets_plan](../c/create_groupingsets_plan.md)

## Notes and Other Information
- The function is static, used only within createplan.c
- Requires that root->grouping_map is already set up (asserted with Assert(grouping_map))
- Allocates memory for the new array using palloc0, ensuring zero-initialization
- The returned array has the same length as the input groupClause list
- Essential for implementing SQL GROUPING SETS, ROLLUP, and CUBE operations where column references need to be remapped