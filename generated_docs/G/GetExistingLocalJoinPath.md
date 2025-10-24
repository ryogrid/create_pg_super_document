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

## Simplified Source

```c
Path *GetExistingLocalJoinPath(RelOptInfo *joinrel)
{
    ListCell *lc;

    Assert(IS_JOIN_REL(joinrel));

    // Search through all paths in the join relation
    foreach(lc, joinrel->pathlist)
    {
        Path *path = (Path *) lfirst(lc);
        JoinPath *joinpath = NULL;

        // Skip parameterized paths - only handle unparameterized joins
        if (path->param_info != NULL)
            continue;

        // Create shallow copy based on join type
        switch (path->pathtype)
        {
            case T_HashJoin:
                {
                    HashPath *hash_path = makeNode(HashPath);
                    memcpy(hash_path, path, sizeof(HashPath));
                    joinpath = (JoinPath *) hash_path;
                }
                break;

            case T_NestLoop:
                {
                    NestPath *nest_path = makeNode(NestPath);
                    memcpy(nest_path, path, sizeof(NestPath));
                    joinpath = (JoinPath *) nest_path;
                }
                break;

            case T_MergeJoin:
                {
                    MergePath *merge_path = makeNode(MergePath);
                    memcpy(merge_path, path, sizeof(MergePath));
                    joinpath = (JoinPath *) merge_path;
                }
                break;

            default:
                // Skip unsupported path types
                break;
        }

        if (!joinpath)
            continue;

        // Replace foreign subpaths with local equivalents for EPQ compatibility
        if (IsA(joinpath->outerjoinpath, ForeignPath))
        {
            ForeignPath *foreign_path = (ForeignPath *) joinpath->outerjoinpath;
            if (IS_JOIN_REL(foreign_path->path.parent))
                joinpath->outerjoinpath = foreign_path->fdw_outerpath;
        }

        if (IsA(joinpath->innerjoinpath, ForeignPath))
        {
            ForeignPath *foreign_path = (ForeignPath *) joinpath->innerjoinpath;
            if (IS_JOIN_REL(foreign_path->path.parent))
                joinpath->innerjoinpath = foreign_path->fdw_outerpath;
        }

        return (Path *) joinpath;
    }

    // No suitable local join path found
    return NULL;
}
```