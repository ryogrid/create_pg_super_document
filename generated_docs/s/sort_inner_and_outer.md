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
- : PlannerInfo structure containing planner state and configuration
- : RelOptInfo for the join relation being planned
- : RelOptInfo for the outer join relation
- : RelOptInfo for the inner join relation
- : Type of join operation to perform
- : JoinPathExtraData containing additional input values including mergeclause list

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