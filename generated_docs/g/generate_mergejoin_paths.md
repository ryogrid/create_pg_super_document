# generate_mergejoin_paths

## Location
src/backend/optimizer/path/joinpath.c: 1469 - 1716

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
  - find_mergeclauses_for_outer_pathkeys
  - make_inner_pathkeys_for_merge
  - try_mergejoin_path
  - pathkeys_contained_in
  - get_cheapest_path_for_pathkeys
  - compare_path_costs
  - trim_mergeclauses_for_inner_pathkeys
  - list_truncate
  - list_copy
- Called from (representative examples):
  - match_unsorted_outer
  - consider_parallel_mergejoin

## Notes and Other Information
- This is a static function within joinpath.c specifically for mergejoin path generation
- Handles the special case of clauseless FULL JOIN, which only mergejoin can execute
- Uses sophisticated cost-based pruning to avoid generating obviously inferior paths
- Does not consider parameterized inner paths to prevent combinatorial explosion
- Implements incremental truncation of sort keys rather than exhaustive subset enumeration for performance
- Optimizes memory usage by reusing clause lists when possible
- The truncation strategy balances plan quality with planning time by considering progressively shorter sort key lists