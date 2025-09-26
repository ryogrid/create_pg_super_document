# create_worktablescan_path

## Location
[src/backend/optimizer/util/pathnode.c:2202-2234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L2202-L2234)

## Overview
Creates a path node for scanning a self-reference CTE (Common Table Expression), specifically designed for recursive CTE operations where the CTE references itself.

## Definition

```c
Path *
create_worktablescan_path(PlannerInfo *root, RelOptInfo *rel,
						  Relids required_outer)
```
## Detailed Description
This function constructs a Path node specifically for work table scan operations, which are used in recursive CTEs when the CTE needs to reference itself. Work tables serve as intermediate storage during recursive CTE evaluation, holding the results from each iteration of the recursion. The function sets the pathtype to T_WorkTableScan and initializes all necessary Path structure fields. Interestingly, it reuses the cost_ctescan function for cost calculation, as the cost model is similar to regular CTE scans. Like other scan types that deal with computed or temporary data, work table scans produce unordered results.

## Parameters / Member Variables
- : PlannerInfo structure containing global information about the query being planned
- : RelOptInfo structure representing the work table relation being scanned
- : Set of relation IDs that must be available as outer relations for this path

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [get_baserel_parampathinfo](../g/get_baserel_parampathinfo.md)
  - [cost_ctescan](cost_ctescan.md)
- Called from (representative examples):
  - [set_worktable_pathlist](../s/set_worktable_pathlist.md)

## Notes and Other Information
- Sets pathtype to T_WorkTableScan to identify this as a work table scan path
- Always sets pathkeys to NIL because work table results are inherently unordered
- The path is marked as not parallel-aware but respects the relation's parallel safety settings
- No parallel workers are assigned (parallel_workers = 0)
- Reuses cost_ctescan for cost calculation since work tables have similar cost characteristics to regular CTEs
- Essential for implementing recursive CTEs where the CTE definition references itself
- Work tables act as temporary storage for intermediate results during recursive evaluation