# GetExistingLocalJoinPath

## Location
[src/backend/foreign/foreign.c:741-826](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/foreign/foreign.c#L741-L826)

## Overview
Returns a shallow copy of an existing local join path for a given join relation, primarily used to obtain an alternative local path for EPQ (Executor Per-Query) checks when dealing with foreign joins.

## Definition

```c
Path *
GetExistingLocalJoinPath(RelOptInfo *joinrel)
```
## Detailed Description
This function searches through the pathlist of a join relation to find a suitable local join path that can be used as an alternative to foreign join paths. It specifically looks for unparameterized paths of types MergeJoin, HashJoin, or NestLoop since these are the only join types that can be used to construct local plans for foreign joins.

The function creates shallow copies of the chosen paths to avoid issues with the planner potentially freeing the original paths later. If the inner or outer subpaths are ForeignPath nodes representing pushed-down joins, they are replaced with their fdw_outerpath to ensure the returned path consists entirely of local join strategies.

The primary use case is for EPQ checks, where PostgreSQL needs a local execution plan as a fallback when foreign data wrappers cannot handle certain operations or when row-level security checks are required.

## Parameters / Member Variables
- `*joinrel`: A RelOptInfo structure representing the join relation for which to find an existing local join path
## Dependencies
- Functions called/Symbols referenced:
  - IS_JOIN_REL (macro for checking if relation is a join)
  - makeNode (for creating new path nodes)
  - memcpy (for shallow copying path structures)
  - lfirst (for list iteration)
  - IsA (for type checking path nodes)
- Types referenced:
  - [Path](../P/Path.md), JoinPath, HashPath, NestPath, MergePath, ForeignPath
  - [RelOptInfo](../R/RelOptInfo.md), ListCell
- Called from (representative examples):
  - No direct callers found in current analysis

## Notes and Other Information
- Only supports unparameterized foreign joins currently
- Efficiency is not a primary concern since the path is intended for EPQ checks
- Returns NULL if no suitable local join path is found
- The function relies on the pathlist being sorted by total cost, naturally selecting more efficient paths
- Handles three specific join types: T_HashJoin, T_NestLoop, and T_MergeJoin
- Automatically replaces foreign subpaths with their local equivalents to maintain purely local execution strategies