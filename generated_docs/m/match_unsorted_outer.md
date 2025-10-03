# match_unsorted_outer

## Location
[src/backend/optimizer/path/joinpath.c:1717-1968](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinpath.c#L1717-L1968)

## Overview
Creates possible join paths by generating nestloop and mergejoin paths for each available outer path, considering various inner path options and optimization strategies.

## Definition
```c
static void match_unsorted_outer(PlannerInfo *root,
                                 RelOptInfo *joinrel,
                                 RelOptInfo *outerrel,
                                 RelOptInfo *innerrel,
                                 JoinType jointype,
                                 JoinPathExtraData *extra)
```

## Detailed Description
This function is a comprehensive join path generator that creates multiple types of join paths for processing a single join relation. It generates nestloop paths for each available outer path and considers mergejoin paths when appropriate clauses are available.

For nestloop paths, the function generates up to five variants per outer path:
1. One on the cheapest-total-cost inner path
2. One with materialization of the cheapest inner path
3. One on the cheapest-startup-cost inner path (if different)
4. One on the cheapest-total inner-indexscan path (if available)
5. One on the cheapest-startup inner-indexscan path (if different)

The function also handles special cases:
- Unique join types are converted to inner joins with unique path creation
- Different join types have different restrictions (nestloop only supports INNER, LEFT, SEMI, ANTI)
- RIGHT, RIGHT_ANTI, and FULL joins require all mergeclauses to be used
- Parameterized paths are carefully managed to avoid invalid combinations
- Parallel execution paths are considered when conditions allow

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner state and configuration
- `*joinrel`: RelOptInfo for the join relation being planned
- `*outerrel`: RelOptInfo for the outer join relation
- `*innerrel`: RelOptInfo for the inner join relation
- `jointype`: Type of join operation to perform
- `*extra`: JoinPathExtraData containing additional input values
## Dependencies
- Functions called/Symbols referenced:
  - PATH_PARAM_BY_REL
  - [create_unique_path](../c/create_unique_path.md)
  - [ExecMaterializesOutput](../E/ExecMaterializesOutput.md)
  - [create_material_path](../c/create_material_path.md)
  - [build_join_pathkeys](../b/build_join_pathkeys.md)
  - [try_nestloop_path](../t/try_nestloop_path.md)
  - [get_memoize_path](../g/get_memoize_path.md)
  - [generate_mergejoin_paths](../g/generate_mergejoin_paths.md)
  - [consider_parallel_nestloop](../c/consider_parallel_nestloop.md)
  - [consider_parallel_mergejoin](../c/consider_parallel_mergejoin.md)
  - [get_cheapest_parallel_safe_total_inner](../g/get_cheapest_parallel_safe_total_inner.md)
  - bms_is_empty
- Called from (representative examples):
  - [add_paths_to_joinrel](../a/add_paths_to_joinrel.md)

## Notes and Other Information
- This is a static function within joinpath.c serving as a major join path generation hub
- Implements comprehensive join type validation with different restrictions per join type
- Uses sophisticated parameterization checking to ensure path validity
- Supports memoization optimization for nested loop joins to cache repeated inner relation scans
- Considers both sequential and parallel execution paths with appropriate restrictions
- Handles material path creation as an optimization for repeated inner relation access
- The function balances comprehensive path generation with performance by avoiding obviously poor combinations
- Cannot handle certain join types (JOIN_FULL, JOIN_RIGHT, JOIN_RIGHT_ANTI) in parallel mode due to potential correctness issues

## Simplified Source

```c
static void match_unsorted_outer(PlannerInfo *root,
                                RelOptInfo *joinrel,
                                RelOptInfo *outerrel,
                                RelOptInfo *innerrel,
                                JoinType jointype,
                                JoinPathExtraData *extra)
{
    JoinType save_jointype = jointype;
    bool nestjoinOK;
    bool useallclauses;
    Path *inner_cheapest_total = innerrel->cheapest_total_path;
    Path *matpath = NULL;
    ListCell *lc1;

    // Determine join type capabilities
    switch (jointype)
    {
        case JOIN_INNER:
        case JOIN_LEFT:
        case JOIN_SEMI:
        case JOIN_ANTI:
            nestjoinOK = true;
            useallclauses = false;
            break;
        case JOIN_RIGHT:
        case JOIN_RIGHT_ANTI:
        case JOIN_FULL:
            nestjoinOK = false;
            useallclauses = true;
            break;
        case JOIN_UNIQUE_OUTER:
        case JOIN_UNIQUE_INNER:
            jointype = JOIN_INNER;
            nestjoinOK = true;
            useallclauses = false;
            break;
        default:
            elog(ERROR, "unrecognized join type: %d", (int) jointype);
    }

    // Skip inner path if parameterized by outer rel
    if (PATH_PARAM_BY_REL(inner_cheapest_total, outerrel))
        inner_cheapest_total = NULL;

    // Handle unique inner join case
    if (save_jointype == JOIN_UNIQUE_INNER)
    {
        if (inner_cheapest_total == NULL)
            return;
        inner_cheapest_total = create_unique_path(root, innerrel,
                                                inner_cheapest_total, extra->sjinfo);
    }
    else if (nestjoinOK)
    {
        // Consider materializing cheapest inner path
        if (enable_material && inner_cheapest_total != NULL &&
            !ExecMaterializesOutput(inner_cheapest_total->pathtype))
            matpath = create_material_path(innerrel, inner_cheapest_total);
    }

    // Try each outer path
    foreach(lc1, outerrel->pathlist)
    {
        Path *outerpath = (Path *) lfirst(lc1);
        List *merge_pathkeys;

        // Skip outer path if parameterized by inner rel
        if (PATH_PARAM_BY_REL(outerpath, innerrel))
            continue;

        // Handle unique outer join case
        if (save_jointype == JOIN_UNIQUE_OUTER)
        {
            if (outerpath != outerrel->cheapest_total_path)
                continue;
            outerpath = create_unique_path(root, outerrel, outerpath, extra->sjinfo);
        }

        // Build pathkeys for result ordering
        merge_pathkeys = build_join_pathkeys(root, joinrel, jointype,
                                           outerpath->pathkeys);

        if (save_jointype == JOIN_UNIQUE_INNER)
        {
            // Simple nestloop with unique inner
            try_nestloop_path(root, joinrel, outerpath, inner_cheapest_total,
                            merge_pathkeys, jointype, extra);
        }
        else if (nestjoinOK)
        {
            // Try nestloop with various inner paths
            foreach(lc2, innerrel->cheapest_parameterized_paths)
            {
                Path *innerpath = (Path *) lfirst(lc2);

                try_nestloop_path(root, joinrel, outerpath, innerpath,
                                merge_pathkeys, jointype, extra);

                // Try with memoization
                Path *mpath = get_memoize_path(root, innerrel, outerrel,
                                             innerpath, outerpath, jointype, extra);
                if (mpath)
                    try_nestloop_path(root, joinrel, outerpath, mpath,
                                    merge_pathkeys, jointype, extra);
            }

            // Try materialized inner path
            if (matpath)
                try_nestloop_path(root, joinrel, outerpath, matpath,
                                merge_pathkeys, jointype, extra);
        }

        // Skip further processing for unique outer
        if (save_jointype == JOIN_UNIQUE_OUTER)
            continue;

        // Skip if inner is parameterized by outer
        if (inner_cheapest_total == NULL)
            continue;

        // Generate mergejoin paths
        generate_mergejoin_paths(root, joinrel, innerrel, outerpath,
                               save_jointype, extra, useallclauses,
                               inner_cheapest_total, merge_pathkeys, false);
    }

    // Consider parallel execution if conditions allow
    if (joinrel->consider_parallel &&
        save_jointype != JOIN_UNIQUE_OUTER &&
        save_jointype != JOIN_FULL &&
        save_jointype != JOIN_RIGHT &&
        save_jointype != JOIN_RIGHT_ANTI &&
        outerrel->partial_pathlist != NIL &&
        bms_is_empty(joinrel->lateral_relids))
    {
        // Parallel nestloop
        if (nestjoinOK)
            consider_parallel_nestloop(root, joinrel, outerrel, innerrel,
                                     save_jointype, extra);

        // Find parallel-safe inner path if needed
        if (inner_cheapest_total == NULL || !inner_cheapest_total->parallel_safe)
        {
            if (save_jointype == JOIN_UNIQUE_INNER)
                return;
            inner_cheapest_total = get_cheapest_parallel_safe_total_inner(innerrel->pathlist);
        }

        // Parallel mergejoin
        if (inner_cheapest_total)
            consider_parallel_mergejoin(root, joinrel, outerrel, innerrel,
                                      save_jointype, extra, inner_cheapest_total);
    }
}
```