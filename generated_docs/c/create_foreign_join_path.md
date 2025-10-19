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
- `*root`: PlannerInfo structure containing planner state and context
- `*rel`: RelOptInfo representing the join relation being planned
- `*target`: PathTarget specifying the columns to be returned (NULL defaults to rel->reltarget)
- `rows`: Estimated number of rows the foreign join will return
- `startup_cost`: Estimated cost to begin returning tuples
- `total_cost`: Estimated total cost to return all tuples
- `*pathkeys`: List of SortGroupClause structures representing the sort order
- `required_outer`: Relids of relations required as parameters (currently must be empty)
- `*fdw_outerpath`: FDW-specific outer path information
- `*fdw_restrictinfo`: List of restriction clauses that can be executed remotely
- `*fdw_private`: FDW-private information for execution
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

## Simplified Source

```c
/*
 * Create a path node for foreign join operations.
 * Called by FDW's GetForeignJoinPaths function to represent
 * joins executed on the foreign server.
 */
ForeignPath *
create_foreign_join_path(PlannerInfo *root, RelOptInfo *rel,
                        PathTarget *target, double rows,
                        Cost startup_cost, Cost total_cost,
                        List *pathkeys, Relids required_outer,
                        Path *fdw_outerpath, List *fdw_restrictinfo,
                        List *fdw_private)
{
    // Parameterized foreign joins not yet supported
    if (!bms_is_empty(required_outer) || !bms_is_empty(rel->lateral_relids))
        elog(ERROR, "parameterized foreign joins are not supported yet");

    // Create new ForeignPath node
    ForeignPath *pathnode = makeNode(ForeignPath);

    // Set basic path properties
    pathnode->path.pathtype = T_ForeignScan;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = target ? target : rel->reltarget;
    pathnode->path.param_info = NULL;  // No parameterization yet

    // Set parallel execution properties
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel;
    pathnode->path.parallel_workers = 0;

    // Set cost estimates (provided by FDW)
    pathnode->path.rows = rows;
    pathnode->path.startup_cost = startup_cost;
    pathnode->path.total_cost = total_cost;
    pathnode->path.pathkeys = pathkeys;

    // Set FDW-specific fields
    pathnode->fdw_outerpath = fdw_outerpath;
    pathnode->fdw_restrictinfo = fdw_restrictinfo;
    pathnode->fdw_private = fdw_private;

    return pathnode;
}
```