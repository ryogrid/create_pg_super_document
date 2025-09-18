# match_unsorted_outer

## Location
src/backend/optimizer/path/joinpath.c: 1717 - 1968

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
- : PlannerInfo structure containing planner state and configuration
- : RelOptInfo for the join relation being planned  
- : RelOptInfo for the outer join relation
- : RelOptInfo for the inner join relation
- : Type of join operation to perform
- : JoinPathExtraData containing additional input values

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