# create_foreign_upper_path

## Location
[src/backend/optimizer/util/pathnode.c:2333-2377](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L2333-L2377)

## Overview
Creates a path node corresponding to an upper relation (e.g., aggregate, window functions, grouping) that is computed directly by a foreign data wrapper on the remote server.

## Definition
```c
ForeignPath *create_foreign_upper_path(PlannerInfo *root, RelOptInfo *rel,
                                      PathTarget *target,
                                      double rows, Cost startup_cost, Cost total_cost,
                                      List *pathkeys,
                                      Path *fdw_outerpath,
                                      List *fdw_restrictinfo,
                                      List *fdw_private)
```

## Detailed Description
This function creates a ForeignPath node representing an upper-level operation (such as aggregation, grouping, or window functions) that will be executed directly on the foreign server. It is exclusively called by foreign data wrappers' GetForeignUpperPaths functions, not by core PostgreSQL. The FDW must provide all cost estimates and path properties since core PostgreSQL cannot calculate them for foreign operations. Upper relations are assumed to have completed all joining operations, so no lateral references should exist.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context
- `rel`: RelOptInfo representing the upper relation being planned
- `target`: PathTarget specifying the columns to be returned (NULL defaults to rel->reltarget)
- `rows`: Estimated number of rows the foreign upper operation will return
- `startup_cost`: Estimated cost to begin returning tuples
- `total_cost`: Estimated total cost to return all tuples
- `pathkeys`: List of SortGroupClause structures representing the sort order
- `fdw_outerpath`: FDW-specific outer path information
- `fdw_restrictinfo`: List of restriction clauses that can be executed remotely
- `fdw_private`: FDW-private information for execution

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create ForeignPath)
  - bms_is_empty (to verify no lateral references)
  - Assert (for debugging assertions)
- Called from (representative examples):
  - FDW-specific GetForeignUpperPaths functions (external to core)

## Notes and Other Information
- This function is part of the foreign data wrapper API for upper-level operations like aggregation and grouping
- Unlike create_foreign_join_path, this function uses Assert rather than elog for lateral reference checking, assuming proper usage
- Upper relations should never have lateral references since all joining is complete at this stage
- The function sets parallel_aware to false and parallel_workers to 0, indicating limited parallel execution support
- If target is NULL, the function defaults to using rel->reltarget
- The created path has pathtype T_ForeignScan and param_info set to NULL