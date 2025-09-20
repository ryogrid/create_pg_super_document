# have_unsafe_outer_join_ref

## Location
[src/backend/optimizer/path/joinpath.c:390-438](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinpath.c#L390-L438)

## Overview
Checks whether a parameterized nested loop join would create unsafe references to outer join relations that could produce incorrect query results.

## Definition

```c
static inline bool
have_unsafe_outer_join_ref(PlannerInfo *root,
						   Relids outerrelids,
						   Relids inner_paramrels)
```
## Detailed Description
This function performs a critical safety check when considering parameterized nested loop joins involving outer joins. It determines whether the proposed join would create unsafe references where parameters from outer join relations are passed down in a way that could violate SQL semantics.

The function works by analyzing the set of unsatisfied parameters (those required by the inner path but not provided by the outer relation). If any of these unsatisfied parameters come from outer join relations, the function checks whether the satisfied parameters (those provided by the outer relation) would create problematic interactions with the special join constraints.

Specifically, it identifies unsafe situations where:
1. The satisfied parameters overlap with the minimum right-hand side of an outer join constraint
2. For full outer joins, the satisfied parameters overlap with the minimum left-hand side

These conditions indicate that the parameterized join could produce incorrect results by changing the semantics of NULL-generation in outer joins.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimizer state including outer join information
- : Relids bitmap representing the set of base relations in the outer side of the join
- : Relids bitmap representing the set of relations that the inner path requires as parameters

## Dependencies
- Functions called/Symbols referenced:
  - [bms_difference](../b/bms_difference.md)
  - [bms_intersect](../b/bms_intersect.md)
  - [bms_overlap](../b/bms_overlap.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_free](../b/bms_free.md)
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md)
  - JOIN_FULL
- Called from (representative examples):
  - [try_nestloop_path](../t/try_nestloop_path.md)

## Notes and Other Information
This function is marked as static inline and is used internally within the joinpath.c module as part of the nested loop join path validation logic. It's essential for maintaining query correctness when dealing with complex outer join scenarios.

The function includes memory management by explicitly freeing the temporary Relids bitmaps (unsatisfied and satisfied) to avoid memory waste when rejecting paths. This is particularly important since join planning can evaluate many potential paths.

The safety check is specifically designed to prevent parameterized joins that would change the NULL-generation semantics of outer joins, which could lead to incorrect query results that are difficult to detect.