# sort_inner_and_outer

## Location
[src/backend/optimizer/path/joinpath.c:1266-1468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinpath.c#L1266-L1468)

## Overview
Creates mergejoin join paths by explicitly sorting both the outer and inner join relations on each available merge ordering.

## Definition
```c
static void sort_inner_and_outer(PlannerInfo *root,
                                 RelOptInfo *joinrel,
                                 RelOptInfo *outerrel,
                                 RelOptInfo *innerrel,
                                 JoinType jointype,
                                 JoinPathExtraData *extra)
```

## Detailed Description
This function generates mergejoin paths by considering explicit sorting of both input relations. It focuses on the cheapest-total-cost input paths under the assumption that sorting will be required. The function handles various join types and considers multiple merge orderings to optimize for potential higher-level mergejoins.

Key operations include:
1. Validates that input paths are not parameterized by each other (incompatible with mergejoin)
2. Handles unique join types by creating unique paths and converting to inner joins
3. Considers partial mergejoin paths for parallel execution when conditions allow
4. Generates multiple orderings of mergeclauses to create differently-sorted result paths
5. Uses heuristics to select promising pathkey orderings rather than exhaustively trying all permutations

The function intentionally avoids parameterized input paths (except cheapest-total) to prevent combinatorial explosion of paths of dubious value.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner state and configuration
- `*joinrel`: RelOptInfo for the join relation being planned
- `*outerrel`: RelOptInfo for the outer join relation
- `*innerrel`: RelOptInfo for the inner join relation
- `jointype`: Type of join operation to perform
- `*extra`: JoinPathExtraData containing additional input values including mergeclause list
## Dependencies
- Functions called/Symbols referenced:
  - PATH_PARAM_BY_REL
  - [create_unique_path](../c/create_unique_path.md)
  - bms_is_empty
  - [get_cheapest_parallel_safe_total_inner](../g/get_cheapest_parallel_safe_total_inner.md)
  - [select_outer_pathkeys_for_merge](select_outer_pathkeys_for_merge.md)
  - [find_mergeclauses_for_outer_pathkeys](../f/find_mergeclauses_for_outer_pathkeys.md)
  - [make_inner_pathkeys_for_merge](../m/make_inner_pathkeys_for_merge.md)
  - [build_join_pathkeys](../b/build_join_pathkeys.md)
  - [try_mergejoin_path](../t/try_mergejoin_path.md)
  - [try_partial_mergejoin_path](../t/try_partial_mergejoin_path.md)
- Called from (representative examples):
  - [add_paths_to_joinrel](../a/add_paths_to_joinrel.md)

## Notes and Other Information
- This is a static function within joinpath.c focused on explicit sorting scenarios
- Handles special join types (JOIN_UNIQUE_OUTER, JOIN_UNIQUE_INNER) by creating unique paths
- Supports partial mergejoin paths for parallel execution, but with restrictions on join types
- Uses sophisticated pathkey ordering heuristics rather than brute-force permutation testing
- Part of PostgreSQL's mergejoin path generation infrastructure
- The function balances thoroughness with planning time by limiting the number of orderings considered
- Cannot handle certain join types (JOIN_FULL, JOIN_RIGHT, JOIN_RIGHT_ANTI) in parallel mode due to potential false null extended rows

## Simplified Source

```c
static void sort_inner_and_outer(PlannerInfo *root,
                                RelOptInfo *joinrel,
                                RelOptInfo *outerrel,
                                RelOptInfo *innerrel,
                                JoinType jointype,
                                JoinPathExtraData *extra)
{
    JoinType save_jointype = jointype;
    Path *outer_path = outerrel->cheapest_total_path;
    Path *inner_path = innerrel->cheapest_total_path;
    Path *cheapest_partial_outer = NULL;
    Path *cheapest_safe_inner = NULL;
    List *all_pathkeys;
    ListCell *l;

    // Can't use mergejoin if paths are parameterized by each other
    if (PATH_PARAM_BY_REL(outer_path, innerrel) ||
        PATH_PARAM_BY_REL(inner_path, outerrel))
        return;

    // Handle unique join types
    if (jointype == JOIN_UNIQUE_OUTER)
    {
        outer_path = create_unique_path(root, outerrel, outer_path, extra->sjinfo);
        jointype = JOIN_INNER;
    }
    else if (jointype == JOIN_UNIQUE_INNER)
    {
        inner_path = create_unique_path(root, innerrel, inner_path, extra->sjinfo);
        jointype = JOIN_INNER;
    }

    // Set up for parallel execution if conditions allow
    if (joinrel->consider_parallel &&
        save_jointype != JOIN_UNIQUE_OUTER &&
        save_jointype != JOIN_FULL &&
        save_jointype != JOIN_RIGHT &&
        save_jointype != JOIN_RIGHT_ANTI &&
        outerrel->partial_pathlist != NIL &&
        bms_is_empty(joinrel->lateral_relids))
    {
        cheapest_partial_outer = linitial(outerrel->partial_pathlist);

        if (inner_path->parallel_safe)
            cheapest_safe_inner = inner_path;
        else if (save_jointype != JOIN_UNIQUE_INNER)
            cheapest_safe_inner = get_cheapest_parallel_safe_total_inner(innerrel->pathlist);
    }

    // Get all possible pathkey orderings for merge
    all_pathkeys = select_outer_pathkeys_for_merge(root, extra->mergeclause_list, joinrel);

    // Try each pathkey ordering
    foreach(l, all_pathkeys)
    {
        PathKey *front_pathkey = lfirst(l);
        List *outerkeys;
        List *cur_mergeclauses;
        List *innerkeys;
        List *merge_pathkeys;

        // Create pathkey list with current pathkey first
        if (l != list_head(all_pathkeys))
            outerkeys = lcons(front_pathkey,
                            list_delete_nth_cell(list_copy(all_pathkeys),
                                               foreach_current_index(l)));
        else
            outerkeys = all_pathkeys;

        // Sort mergeclauses to match outer pathkey ordering
        cur_mergeclauses = find_mergeclauses_for_outer_pathkeys(root, outerkeys,
                                                              extra->mergeclause_list);

        // Build corresponding inner pathkeys
        innerkeys = make_inner_pathkeys_for_merge(root, cur_mergeclauses, outerkeys);

        // Build pathkeys for result ordering
        merge_pathkeys = build_join_pathkeys(root, joinrel, jointype, outerkeys);

        // Try sequential mergejoin
        try_mergejoin_path(root, joinrel, outer_path, inner_path,
                         merge_pathkeys, cur_mergeclauses, outerkeys, innerkeys,
                         jointype, extra, false);

        // Try parallel mergejoin if possible
        if (cheapest_partial_outer && cheapest_safe_inner)
            try_partial_mergejoin_path(root, joinrel,
                                     cheapest_partial_outer, cheapest_safe_inner,
                                     merge_pathkeys, cur_mergeclauses,
                                     outerkeys, innerkeys, jointype, extra);
    }
}
```