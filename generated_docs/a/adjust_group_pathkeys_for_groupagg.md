# adjust_group_pathkeys_for_groupagg

## Location
src/backend/optimizer/plan/planner.c: 3285 - 3508

## Overview
Optimizes pathkeys for ordered aggregates by finding the best set of pre-ordered input that suits the largest number of aggregate functions, reducing sorting overhead.

## Definition
```c
static void adjust_group_pathkeys_for_groupagg(PlannerInfo *root)
```

## Detailed Description
This function analyzes ordered aggregate functions (those with ORDER BY or DISTINCT clauses) to determine the optimal set of pathkeys that can satisfy the largest number of aggregates with a single sort operation. The algorithm aims to minimize the total number of sorts required across all aggregates.

The process works in several phases:
1. **Collection Phase**: Identifies all processable aggregates (skipping ordered sets, those without ORDER BY/DISTINCT, and unsafe filtered aggregates)
2. **Safety Checks**: For aggregates with FILTER clauses, ensures sorting expressions won't cause errors before filtering
3. **Optimization Loop**: Iteratively finds the best set of pathkeys by:
   - Starting with the first unprocessed aggregate's pathkeys
   - Finding compatible aggregates with same/compatible pathkeys
   - Selecting stronger pathkeys when available (via `compare_pathkeys`)
   - Repeating until no better combination exists
4. **Volatile Function Handling**: Excludes aggregates with volatile functions to ensure consistent behavior
5. **Finalization**: Updates `root->group_pathkeys` and marks compatible aggregates as `aggpresorted = true`

The function ensures that aggregates with volatile functions perform independent sorts to avoid inconsistent results that could depend on query structure.

## Parameters / Member Variables
- `root`: PlannerInfo containing query planning context, aggregate information, and pathkeys to be updated

## Dependencies
- Functions called/Symbols referenced:
  - [AggInfo](../A/AggInfo.md)
  - Aggref
  - AGGKIND_IS_ORDERED_SET
  - RelabelType
  - [make_pathkeys_for_sortclauses](../m/make_pathkeys_for_sortclauses.md)
  - [has_volatile_pathkey](../h/has_volatile_pathkey.md)
  - [compare_pathkeys](../c/compare_pathkeys.md)
  - [append_pathkeys](append_pathkeys.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_del_member](../b/bms_del_member.md)
  - [bms_del_members](../b/bms_del_members.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [bms_num_members](../b/bms_num_members.md)
  - list_nth_node
  - linitial_node
  - [list_copy](../l/list_copy.md)
- Called from (representative examples):
  - [standard_qp_callback](../s/standard_qp_callback.md)

## Notes and Other Information
- Only operates when `enable_presorted_aggregate` is enabled and `numOrderedAggs > 0`
- Requires no grouping sets present (`groupingSets == NIL`)
- For filtered aggregates, only allows Vars and Consts in arguments to prevent sorting errors
- Uses bipartite matching logic to find optimal aggregate groupings
- Volatile functions are explicitly excluded to maintain deterministic behavior across different query structures
- The `aggpresorted` flag tells the executor to skip sorting for optimized aggregates
- Works in conjunction with `create_grouping_paths` which avoids hash aggregates when ordered aggregates are present
- Pathkey comparison results (BETTER1, BETTER2, EQUAL, DIFFERENT) determine aggregate compatibility