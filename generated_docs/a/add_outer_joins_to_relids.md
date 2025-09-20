# add_outer_joins_to_relids

## Location
[src/backend/optimizer/path/joinrels.c:802-893](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L802-L893)

## Overview
Modifies input relation IDs to include relids of outer joins that will be calculated at the current join level, handling complex outer join ordering rules.

## Definition

```c
Relids
add_outer_joins_to_relids(PlannerInfo *root, Relids input_relids,
						  SpecialJoinInfo *sjinfo,
						  List **pushed_down_joins)
```
## Detailed Description
The  function is responsible for managing the complex logic of outer join ordering and dependencies in PostgreSQL's query optimizer. It takes the union of relid sets from two input relations and adds additional relids to represent outer joins that will be computed at this join level. The function implements outer-join identity 3 rules, which allow certain outer joins to be reordered or "pushed down" for optimization purposes.

The function handles the intricate rules governing when outer joins can be executed in non-syntactic order. It checks various conditions including commutation constraints and ensures that pushed-down outer joins are properly represented in the final relid set when their computation is completed. This is crucial for maintaining the semantic correctness of outer join operations while allowing the optimizer flexibility in join ordering.

## Parameters / Member Variables
- : The PlannerInfo structure containing global planning information and join_info_list
- : The union of relid sets from the two input relations being joined (modified in-place)
- : SpecialJoinInfo representing the join currently being performed
- : Optional output parameter to collect SpecialJoinInfos for added outer joins (must be initialized to NIL by caller)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_is_subset](../b/bms_is_subset.md)  
  - [bms_copy](../b/bms_copy.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_add_members](../b/bms_add_members.md)
- Called from (representative examples):
  - [make_join_rel](../m/make_join_rel.md)
  - [generate_join_implied_equalities](../g/generate_join_implied_equalities.md)

## Notes and Other Information
- Returns early if the join is not an outer join or has no assigned relid
- Only processes LEFT JOIN types for reordering - other join types use syntactic ordering
- Implements complex logic for outer join identity 3 which allows pushing joins into RHS of syntactically-lower left joins
- The function modifies input_relids in-place and returns it; callers need bms_copy() if they want to preserve the original
- Handles cascading effects where adding one outer join may enable adding others through commute_above_l relationships
- Critical for maintaining correct outer join semantics while enabling query optimization flexibility