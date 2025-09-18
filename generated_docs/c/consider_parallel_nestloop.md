# consider_parallel_nestloop

## Location
src/backend/optimizer/path/joinpath.c: 2009 - 2092

## Overview
Attempts to build partial paths for a join relation by combining partial paths from the outer relation with complete paths from the inner relation using nested loop join algorithm in a parallel context.

## Definition
```c
static void consider_parallel_nestloop(PlannerInfo *root,
                                      RelOptInfo *joinrel,
                                      RelOptInfo *outerrel,
                                      RelOptInfo *innerrel,
                                      JoinType jointype,
                                      JoinPathExtraData *extra)
```

## Detailed Description
This function creates parallel-aware nested loop join paths by combining partial paths from the outer relation with parameterized and unparameterized paths from the inner relation. Unlike merge joins, nested loop joins don't require pre-sorted input, making them more flexible but potentially less efficient for large datasets.

The function handles special join types like JOIN_UNIQUE_INNER by applying uniqueness constraints to the inner path. It also explores memoization opportunities to cache inner relation results and improve performance when the same inner tuples are accessed multiple times by different outer tuples.

For each partial outer path, the function examines all suitable inner paths, ensuring they are parallel-safe before attempting to create join paths. The function leverages PostgreSQL's memoization feature when beneficial.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and context
- `joinrel`: The target join relation for which paths are being generated
- `outerrel`: The outer relation in the join operation
- `innerrel`: The inner relation in the join operation
- `jointype`: The type of join operation (INNER, LEFT, RIGHT, UNIQUE_INNER, etc.)
- `extra`: Additional join-specific data and constraints

## Dependencies
- Functions called/Symbols referenced:
  - JoinType (enum type)
  - JoinPathExtraData (struct type)
  - JOIN_UNIQUE_INNER (enum value)
  - JOIN_INNER (enum value)
  - build_join_pathkeys
  - create_unique_path
  - try_partial_nestloop_path
  - get_memoize_path
- Called from (representative examples):
  - match_unsorted_outer

## Notes and Other Information
- This is a static function, meaning it's only accessible within the joinpath.c compilation unit
- The function specifically handles JOIN_UNIQUE_INNER by converting it to JOIN_INNER and applying uniqueness through create_unique_path
- Parallel safety is enforced - only parallel-safe inner paths are considered
- The function explores memoization opportunities to optimize repeated access to inner relation tuples
- For JOIN_UNIQUE_INNER, only the cheapest total path of the inner relation is used due to limitations in create_unique_path
- The function processes all parameterized paths from the inner relation, but only those producing unparameterized results survive the join operation