# prepare_sort_from_pathkeys

## Location
[src/backend/optimizer/plan/createplan.c:6165-6346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L6165-L6346)

## Overview
The prepare_sort_from_pathkeys function prepares the sorting metadata and potentially modifies the input plan to support sorting operations, converting pathkey specifications into executor-ready sort arrays.

## Definition
```c
static Plan *
prepare_sort_from_pathkeys(Plan *lefttree, List *pathkeys,
                           Relids relids,
                           const AttrNumber *reqColIdx,
                           bool adjust_tlist_in_place,
                           int *p_numsortkeys,
                           AttrNumber **p_sortColIdx,
                           Oid **p_sortOperators,
                           Oid **p_collations,
                           bool **p_nullsFirst)
```

## Detailed Description
The prepare_sort_from_pathkeys function is a comprehensive utility that converts high-level pathkey specifications into the low-level arrays required by PostgreSQL's sorting executor nodes (Sort, MergeAppend, and Gather Merge). It handles the complex process of mapping equivalence classes and expressions to concrete sort column indices, operators, and settings.

The function performs several critical operations:
1. Iterates through the pathkeys list to build sort specification arrays
2. Handles volatile equivalence classes that must match specific ORDER BY clauses
3. Manages required column matching for MergeAppend child plans
4. Searches for existing targetlist entries that match pathkey expressions
5. Creates resjunk targetlist entries for expressions not already computed
6. Potentially injects Result nodes when the input plan cannot perform projections
7. Resolves sort operators from pathkey operator families

This function is essential for bridging the gap between the optimizer's abstract pathkey representation and the executor's concrete sort requirements.

## Parameters / Member Variables
- `lefttree`: The input plan node that provides tuples to be sorted
- `pathkeys`: List of PathKey objects specifying the desired sort order
- `relids`: Set of relation IDs to constrain equivalence class member matching
- `reqColIdx`: Optional array of required sort column numbers for MergeAppend children
- `adjust_tlist_in_place`: Whether to modify the lefttree's targetlist directly
- `p_numsortkeys`: Output parameter for the number of sort columns
- `p_sortColIdx`: Output parameter for array of sort column indices
- `p_sortOperators`: Output parameter for array of sort operator OIDs
- `p_collations`: Output parameter for array of collation OIDs
- `p_nullsFirst`: Output parameter for array of null ordering flags

## Dependencies
- Functions called/Symbols referenced:
  - [get_sortgroupref_tle](../g/get_sortgroupref_tle.md) (to find targetlist entries by sort reference)
  - [get_tle_by_resno](../g/get_tle_by_resno.md) (to find targetlist entries by result number)
  - [find_ec_member_matching_expr](../f/find_ec_member_matching_expr.md) (to match equivalence class members to expressions)
  - [find_computable_ec_member](../f/find_computable_ec_member.md) (to find computable equivalence class members)
  - [is_projection_capable_plan](../i/is_projection_capable_plan.md) (to check if plan can perform projections)
  - [inject_projection_plan](../i/inject_projection_plan.md) (to add Result node for projection)
  - [get_opfamily_member](../g/get_opfamily_member.md) (to resolve sort operators)
  - [makeTargetEntry](../m/makeTargetEntry.md) (to create resjunk targetlist entries)
  - copyObject (to duplicate objects safely)
- Called from (representative examples):
  - [create_append_plan](../c/create_append_plan.md) (src/backend/optimizer/plan/createplan.c:1286, 1330)
  - [create_merge_append_plan](../c/create_merge_append_plan.md) (src/backend/optimizer/plan/createplan.c:1471, 1502)
  - [create_gather_merge_plan](../c/create_gather_merge_plan.md) (src/backend/optimizer/plan/createplan.c:1981)
  - [make_sort_from_pathkeys](../m/make_sort_from_pathkeys.md) (src/backend/optimizer/plan/createplan.c:6356)
  - [make_incrementalsort_from_pathkeys](../m/make_incrementalsort_from_pathkeys.md) (src/backend/optimizer/plan/createplan.c:6392)

## Notes and Other Information
- This is a static function within createplan.c, used internally by the planner
- The function may modify the input plan's targetlist or inject additional plan nodes
- Handles complex cases like volatile equivalence classes that require specific ORDER BY matching
- Critical for the correct functioning of merge operations that require sorted input
- May allocate memory for sort specification arrays that must be preserved for plan execution
- The reqColIdx parameter is specifically used when creating child plans for MergeAppend nodes
- Located at src/backend/optimizer/plan/createplan.c:6165-6346