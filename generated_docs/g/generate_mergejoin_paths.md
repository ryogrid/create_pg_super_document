# generate_mergejoin_paths

## Location
[src/backend/optimizer/path/joinpath.c:1469-1716](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinpath.c#L1469-L1716)

## Overview
Creates possible mergejoin paths for a given outer path by finding suitable inner paths and considering truncations of mergeclause lists to optimize join performance.

## Definition
```c
static void generate_mergejoin_paths(PlannerInfo *root,
                                     RelOptInfo *joinrel,
                                     RelOptInfo *innerrel,
                                     Path *outerpath,
                                     JoinType jointype,
                                     JoinPathExtraData *extra,
                                     bool useallclauses,
                                     Path *inner_cheapest_total,
                                     List *merge_pathkeys,
                                     bool is_partial)
```

## Detailed Description
This function generates mergejoin paths using two primary strategies: sorting the cheapest inner path, or using an inner path that is already suitably ordered for the merge. A key optimization is considering truncations of the mergeclause list when no inner path exists for the full list but better paths exist with fewer merge keys.

The function performs several operations:
1. Converts unique join types to inner joins for processing
2. Finds mergeclauses compatible with the outer path's ordering
3. Handles the special case of clauseless FULL JOIN (the only join type that supports FULL JOIN without clauses)
4. Generates a mergejoin path using the cheapest inner path (with sorting if needed)
5. Searches for presorted inner paths that satisfy various truncations of the required sort keys
6. Considers both startup cost and total cost optimizations for inner paths
7. Avoids creating duplicate paths and uses cost comparison to eliminate inferior options

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and configuration
- : RelOptInfo for the join relation being planned
- : RelOptInfo for the inner join relation
- : Path for the outer side of the join
- : Type of join operation to perform
- : JoinPathExtraData containing additional input values including mergeclause list
- : Boolean indicating whether all mergeclauses must be used
- : The cheapest total cost inner path
- : List of pathkeys representing the merge ordering
- : Boolean indicating whether this is for partial (parallel) execution

## Dependencies
- Functions called/Symbols referenced:
  - [find_mergeclauses_for_outer_pathkeys](../f/find_mergeclauses_for_outer_pathkeys.md)
  - [make_inner_pathkeys_for_merge](../m/make_inner_pathkeys_for_merge.md)
  - [try_mergejoin_path](../t/try_mergejoin_path.md)
  - [pathkeys_contained_in](../p/pathkeys_contained_in.md)
  - [get_cheapest_path_for_pathkeys](get_cheapest_path_for_pathkeys.md)
  - [compare_path_costs](../c/compare_path_costs.md)
  - [trim_mergeclauses_for_inner_pathkeys](../t/trim_mergeclauses_for_inner_pathkeys.md)
  - [list_truncate](../l/list_truncate.md)
  - [list_copy](../l/list_copy.md)
- Called from (representative examples):
  - [match_unsorted_outer](../m/match_unsorted_outer.md)
  - [consider_parallel_mergejoin](../c/consider_parallel_mergejoin.md)

## Notes and Other Information
- This is a static function within joinpath.c specifically for mergejoin path generation
- Handles the special case of clauseless FULL JOIN, which only mergejoin can execute
- Uses sophisticated cost-based pruning to avoid generating obviously inferior paths
- Does not consider parameterized inner paths to prevent combinatorial explosion
- Implements incremental truncation of sort keys rather than exhaustive subset enumeration for performance
- Optimizes memory usage by reusing clause lists when possible
- The truncation strategy balances plan quality with planning time by considering progressively shorter sort key lists

## Simplified Source

```c
static void generate_mergejoin_paths(PlannerInfo *root,
                                     RelOptInfo *joinrel,
                                     RelOptInfo *innerrel,
                                     Path *outerpath,
                                     JoinType jointype,
                                     JoinPathExtraData *extra,
                                     bool useallclauses,
                                     Path *inner_cheapest_total,
                                     List *merge_pathkeys,
                                     bool is_partial)
{
    JoinType save_jointype = jointype;

    // Convert unique join types to inner join for processing
    if (jointype == JOIN_UNIQUE_OUTER || jointype == JOIN_UNIQUE_INNER)
        jointype = JOIN_INNER;

    // Find mergeclauses compatible with outer path ordering
    List *mergeclauses = find_mergeclauses_for_outer_pathkeys(root,
                                                              outerpath->pathkeys,
                                                              extra->mergeclause_list);

    // Handle special case: clauseless FULL JOIN (only mergejoin supports this)
    if (mergeclauses == NIL)
    {
        if (jointype != JOIN_FULL)
            return;
    }

    // If we need all clauses, ensure we have them
    if (useallclauses &&
        list_length(mergeclauses) != list_length(extra->mergeclause_list))
        return;

    // Compute required inner path ordering
    List *innersortkeys = make_inner_pathkeys_for_merge(root, mergeclauses,
                                                       outerpath->pathkeys);

    // Try mergejoin with cheapest inner path (will sort if needed)
    try_mergejoin_path(root, joinrel, outerpath, inner_cheapest_total,
                      merge_pathkeys, mergeclauses, NIL, innersortkeys,
                      jointype, extra, is_partial);

    // Early exit for unique inner joins
    if (save_jointype == JOIN_UNIQUE_INNER)
        return;

    // Initialize cost tracking for presorted path search
    Path *cheapest_startup_inner = NULL;
    Path *cheapest_total_inner = NULL;

    if (pathkeys_contained_in(innersortkeys, inner_cheapest_total->pathkeys))
    {
        // Inner path was already sorted
        cheapest_startup_inner = inner_cheapest_total;
        cheapest_total_inner = inner_cheapest_total;
    }

    // Try truncated sort key lists to find better presorted paths
    int num_sortkeys = list_length(innersortkeys);
    List *trialsortkeys = (num_sortkeys > 1 && !useallclauses) ?
                          list_copy(innersortkeys) : innersortkeys;

    for (int sortkeycnt = num_sortkeys; sortkeycnt > 0; sortkeycnt--)
    {
        // Try progressively shorter sort key requirements
        trialsortkeys = list_truncate(trialsortkeys, sortkeycnt);

        // Find cheapest path for current sort requirements
        Path *innerpath = get_cheapest_path_for_pathkeys(innerrel->pathlist,
                                                        trialsortkeys, NULL,
                                                        TOTAL_COST, is_partial);

        if (innerpath != NULL &&
            (cheapest_total_inner == NULL ||
             compare_path_costs(innerpath, cheapest_total_inner, TOTAL_COST) < 0))
        {
            // Create appropriate mergeclause list for this path
            List *newclauses = (sortkeycnt < num_sortkeys) ?
                trim_mergeclauses_for_inner_pathkeys(root, mergeclauses, trialsortkeys) :
                mergeclauses;

            try_mergejoin_path(root, joinrel, outerpath, innerpath,
                              merge_pathkeys, newclauses, NIL, NIL,
                              jointype, extra, is_partial);
            cheapest_total_inner = innerpath;
        }

        // Also check startup cost optimization
        innerpath = get_cheapest_path_for_pathkeys(innerrel->pathlist,
                                                  trialsortkeys, NULL,
                                                  STARTUP_COST, is_partial);

        if (innerpath != NULL && innerpath != cheapest_total_inner &&
            (cheapest_startup_inner == NULL ||
             compare_path_costs(innerpath, cheapest_startup_inner, STARTUP_COST) < 0))
        {
            // Reuse clause list if possible for memory efficiency
            List *newclauses = (sortkeycnt < num_sortkeys) ?
                trim_mergeclauses_for_inner_pathkeys(root, mergeclauses, trialsortkeys) :
                mergeclauses;

            try_mergejoin_path(root, joinrel, outerpath, innerpath,
                              merge_pathkeys, newclauses, NIL, NIL,
                              jointype, extra, is_partial);
            cheapest_startup_inner = innerpath;
        }

        if (useallclauses)
            break;
    }
}
```