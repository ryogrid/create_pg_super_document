# consider_parallel_mergejoin

## Location
[src/backend/optimizer/path/joinpath.c:1969-2008](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinpath.c#L1969-L2008)

## Overview
Attempts to build partial paths for a join relation by combining partial paths from the outer relation with complete paths from the inner relation using merge join algorithm in a parallel context.

## Definition

```c
static void
consider_parallel_mergejoin(PlannerInfo *root,
							RelOptInfo *joinrel,
							RelOptInfo *outerrel,
							RelOptInfo *innerrel,
							JoinType jointype,
							JoinPathExtraData *extra,
							Path *inner_cheapest_total)
```
## Detailed Description
This function is a specialized path generation function that creates parallel-aware merge join paths. It iterates through all available partial paths from the outer relation and attempts to create merge join paths by pairing each partial outer path with the inner relation's complete path. The function leverages the parallel processing capabilities by using partial paths from the outer relation, which can be processed in parallel by multiple workers, while using a complete path for the inner relation that is replicated across all workers.

The function works by determining the appropriate pathkeys (ordering) for the resulting join paths and then delegates the actual path creation to  with the parallel flag set to true.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing global planner state and context
- `*joinrel`: The target join relation for which paths are being generated
- `*outerrel`: The outer relation in the join operation
- `*innerrel`: The inner relation in the join operation
- `jointype`: The type of join operation (INNER, LEFT, RIGHT, etc.)
- `*extra`: Additional join-specific data and constraints
- `*inner_cheapest_total`: The cheapest total cost path for the inner relation
## Dependencies
- Functions called/Symbols referenced:
  - JoinType (enum type)
  - [JoinPathExtraData](../J/JoinPathExtraData.md) (struct type)
  - [build_join_pathkeys](../b/build_join_pathkeys.md)
  - [generate_mergejoin_paths](../g/generate_mergejoin_paths.md)
- Called from (representative examples):
  - [match_unsorted_outer](../m/match_unsorted_outer.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the joinpath.c compilation unit
- The function specifically handles parallel execution scenarios by working with partial paths from the outer relation
- The merge join strategy requires both relations to be ordered on the join keys, which is handled through the pathkeys mechanism
- The function sets the parallel flag to true when calling generate_mergejoin_paths, indicating that the resulting paths should be parallel-aware

## Simplified Source

```c
static void
consider_parallel_mergejoin(PlannerInfo *root,
                            RelOptInfo *joinrel,
                            RelOptInfo *outerrel,
                            RelOptInfo *innerrel,
                            JoinType jointype,
                            JoinPathExtraData *extra,
                            Path *inner_cheapest_total)
{
    ListCell *lc1;

    // Generate merge join paths for each partial outer path
    foreach(lc1, outerrel->partial_pathlist)
    {
        Path *outerpath = (Path *) lfirst(lc1);
        List *merge_pathkeys;

        // Determine the ordering for the resulting join paths
        merge_pathkeys = build_join_pathkeys(root, joinrel, jointype,
                                             outerpath->pathkeys);

        // Create parallel merge join paths using partial outer path
        // and complete inner path
        generate_mergejoin_paths(root, joinrel, innerrel, outerpath, jointype,
                                 extra, false, inner_cheapest_total,
                                 merge_pathkeys, true);  // parallel=true
    }
}
```