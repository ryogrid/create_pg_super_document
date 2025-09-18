# build_join_pathkeys

## Location
src/backend/optimizer/path/pathkeys.c: 1292 - 1329

## Overview
Builds the path keys for a join relation constructed by mergejoin or nestloop join, typically inheriting the outer path's keys but handling special cases for FULL, RIGHT, and RIGHT_ANTI joins.

## Definition


## Detailed Description
The  function determines the appropriate path keys (sort order) for a join relation based on the join type and the outer path's existing keys. In most cases, the result relation maintains the same ordering as the outer path. However, for FULL, RIGHT, and RIGHT_ANTI joins, the function returns NIL (no ordering) because null lefthand rows may be inserted at random points, making the result unsorted.

The function also truncates pathkeys that are uninteresting for higher-level joins using , ensuring that only relevant ordering information is preserved for query optimization at higher levels.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner state and context
- : The join relation (RelOptInfo) that paths are being formed for
- : The type of join operation (inner, left, full, right, etc.)
- : List of path keys from the current outer path

## Dependencies
- Functions called/Symbols referenced:
  - JoinType (enum type)
  - JOIN_FULL (enum constant)
  - JOIN_RIGHT (enum constant)
  - JOIN_RIGHT_ANTI (enum constant)
  - truncate_useless_pathkeys
- Called from (representative examples):
  - sort_inner_and_outer
  - match_unsorted_outer
  - consider_parallel_mergejoin
  - consider_parallel_nestloop

## Notes and Other Information
- The function has been simplified over time since pathkey sublists are now canonicalized from the start
- FULL, RIGHT, and RIGHT_ANTI joins cannot preserve outer path ordering due to potential null row insertion
- The function is critical for maintaining sort order information through join operations in query planning
- Path key truncation helps optimize memory usage and planning efficiency by removing irrelevant ordering constraints