# hash_inner_and_outer

## Location
[src/backend/optimizer/path/joinpath.c:2093-2346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinpath.c#L2093-L2346)

## Overview
Creates hash join paths by explicitly hashing both outer and inner keys of available hash clauses, exploring various combinations of outer and inner paths to find optimal hash join strategies.

## Definition
```c
static void hash_inner_and_outer(PlannerInfo *root,
                                RelOptInfo *joinrel,
                                RelOptInfo *outerrel,
                                RelOptInfo *innerrel,
                                JoinType jointype,
                                JoinPathExtraData *extra)
```

## Detailed Description
This function is responsible for generating hash join paths by systematically examining all possible combinations of outer and inner relation paths. It first identifies usable hash clauses from the join's restriction list, ensuring they are suitable for hash joining and properly match the outer and inner relations.

The function handles various join types differently, applying uniqueness constraints when necessary for JOIN_UNIQUE_OUTER and JOIN_UNIQUE_INNER. It considers both cheapest-total-cost and cheapest-startup-cost paths for comprehensive optimization.

For parallel execution, the function explores partial hash join paths when the join relation is parallel-safe, including shared hash table construction using parallel hash joins when both outer and inner relations have partial paths available.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and context
- `joinrel`: The target join relation for which paths are being generated
- `outerrel`: The outer relation in the join operation
- `innerrel`: The inner relation in the join operation
- `jointype`: The type of join operation (INNER, OUTER, UNIQUE variants, etc.)
- `extra`: Additional join-specific data including restriction clauses

## Dependencies
- Functions called/Symbols referenced:
  - JoinType (enum type)
  - [JoinPathExtraData](../J/JoinPathExtraData.md) (struct type)
  - IS_OUTER_JOIN
  - RINFO_IS_PUSHED_DOWN
  - [clause_sides_match_join](../c/clause_sides_match_join.md)
  - PATH_PARAM_BY_REL
  - JOIN_UNIQUE_OUTER, JOIN_UNIQUE_INNER, JOIN_INNER, JOIN_FULL, JOIN_RIGHT, JOIN_RIGHT_ANTI (enum values)
  - [create_unique_path](../c/create_unique_path.md)
  - [try_hashjoin_path](../t/try_hashjoin_path.md)
  - [try_partial_hashjoin_path](../t/try_partial_hashjoin_path.md)
  - bms_is_empty
  - [get_cheapest_parallel_safe_total_inner](../g/get_cheapest_parallel_safe_total_inner.md)
- Called from (representative examples):
  - [add_paths_to_joinrel](../a/add_paths_to_joinrel.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the joinpath.c compilation unit
- The function filters hash clauses based on join type - outer joins only use their own clauses while inner joins are less restrictive
- [Hash](../H/Hash.md) joins require that neither path is parameterized by the other relation
- For JOIN_UNIQUE_OUTER and JOIN_UNIQUE_INNER, the function applies uniqueness through create_unique_path
- Parallel hash joins are only considered when enable_parallel_hash is true and certain join type restrictions are met
- The function cannot handle certain join types (FULL, RIGHT, RIGHT_ANTI) with parallelism due to match bit distribution requirements
- Parameterized paths are explored systematically, avoiding combinations where paths are parameterized by their counterpart relations