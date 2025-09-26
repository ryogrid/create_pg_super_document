# create_ctescan_path

## Location
[src/backend/optimizer/util/pathnode.c:2124-2149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L2124-L2149)

## Overview
Creates a path node for scanning a non-self-reference Common Table Expression (CTE), which is used during query planning to represent the cost and execution strategy for accessing CTE data.

## Definition

```c
Path *
create_ctescan_path(PlannerInfo *root, RelOptInfo *rel,
					List *pathkeys, Relids required_outer)
```
## Detailed Description
This function constructs a Path node specifically for CTE scan operations. It initializes all the necessary fields of the Path structure with CTE-specific values, including setting the path type to T_CteScan and computing the associated costs. The function handles non-self-reference CTEs, which are CTEs that don't recursively reference themselves. The created path represents one possible execution strategy that the query planner can choose from when determining the optimal query execution plan.

## Parameters / Member Variables
- : PlannerInfo structure containing global information about the query being planned
- : RelOptInfo structure representing the CTE relation being scanned
- : List of PathKey structures specifying the desired output ordering
- : Set of relation IDs that must be available as outer relations for this path

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [get_baserel_parampathinfo](../g/get_baserel_parampathinfo.md)
  - [cost_ctescan](cost_ctescan.md)
- Called from (representative examples):
  - [set_cte_pathlist](../s/set_cte_pathlist.md)

## Notes and Other Information
- Sets pathtype to T_CteScan to identify this as a CTE scan path
- The path is marked as not parallel-aware but respects the relation's parallel safety settings
- No parallel workers are assigned (parallel_workers = 0)
- The cost calculation is delegated to the cost_ctescan function which computes startup and total costs
- This function is part of PostgreSQL's cost-based query optimizer infrastructure