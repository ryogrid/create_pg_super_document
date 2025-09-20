# create_foreign_join_path

## Location
[src/backend/optimizer/util/pathnode.c:2281-2332](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L2281-L2332)

## Overview
Creates a path node corresponding to a scan of a foreign join, allowing foreign data wrappers (FDWs) to represent join operations that will be executed on the foreign server.

## Definition

```c
ForeignPath *
create_foreign_join_path(PlannerInfo *root, RelOptInfo *rel,
						 PathTarget *target,
						 double rows, Cost startup_cost, Cost total_cost,
						 List *pathkeys,
						 Relids required_outer,
						 Path *fdw_outerpath,
						 List *fdw_restrictinfo,
						 List *fdw_private)
```
## Detailed Description
This function creates a ForeignPath node representing a foreign join operation. It is designed to be called exclusively by foreign data wrappers' GetForeignJoinPaths functions, not by core PostgreSQL code. The FDW must supply all cost estimates and path properties since the core system cannot calculate them for foreign operations. The function currently does not support parameterized foreign joins and will throw an error if such paths are attempted.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context
- : RelOptInfo representing the join relation being planned
- : PathTarget specifying the columns to be returned (NULL defaults to rel->reltarget)
- : Estimated number of rows the foreign join will return
- : Estimated cost to begin returning tuples
- : Estimated total cost to return all tuples
- : List of SortGroupClause structures representing the sort order
- : Relids of relations required as parameters (currently must be empty)
- : FDW-specific outer path information
- : List of restriction clauses that can be executed remotely
- : FDW-private information for execution

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create ForeignPath)
  - bms_is_empty (to check parameter requirements)
  - elog (for error reporting)
- Called from (representative examples):
  - FDW-specific GetForeignJoinPaths functions (external to core)

## Notes and Other Information
- This function is part of the foreign data wrapper API and is not used directly by core PostgreSQL
- Parameterized foreign joins are explicitly not supported and will cause an error
- The function sets parallel_aware to false and parallel_workers to 0, indicating limited parallel execution support
- If target is NULL, the function defaults to using rel->reltarget
- The created path has pathtype T_ForeignScan and param_info set to NULL due to parameter limitations