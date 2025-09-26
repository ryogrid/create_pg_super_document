# standard_qp_callback

## Location
[src/backend/optimizer/plan/planner.c:3509-3697](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L3509-L3697)

## Overview
Computes query_pathkeys and other pathkeys during plan generation, determining optimal sorting strategies for grouping, windowing, distinct operations, and ordering.

## Definition
```c
static void standard_qp_callback(PlannerInfo *root, void *extra)
```

## Detailed Description
This callback function is invoked during query planning to compute various pathkey sets that guide the optimizer's decisions about sorting and grouping strategies. It analyzes the query structure and determines the most efficient ordering approach based on the presence of GROUP BY, ORDER BY, DISTINCT, window functions, and set operations.

The function operates through several key phases:

**1. Group Pathkeys Computation**:
- With grouping sets: Uses the first RollupData's groupClause without optimization
- With plain GROUP BY: Removes redundant grouping items using EquivalenceClass processing
- Calls `adjust_group_pathkeys_for_groupagg` for ordered aggregates optimization

**2. Window Pathkeys**: Computes pathkeys for the first (bottom) window function

**3. Distinct Pathkeys**: Processes DISTINCT clauses, removing redundant items via EquivalenceClass analysis

**4. Sort Pathkeys**: Directly converts ORDER BY clauses to pathkeys

**5. Set Operation Pathkeys**: For set operations that benefit from presorted results

**6. Query Pathkeys Priority**: Selects the most beneficial pathkeys in priority order:
   - Group pathkeys (highest priority)
   - Window pathkeys  
   - Distinct pathkeys (if more rigorous than ORDER BY)
   - Sort pathkeys
   - Set operation pathkeys
   - NIL (no specific ordering)

## Parameters / Member Variables
- `root`: PlannerInfo containing the query planning context and pathkey storage
- `extra`: standard_qp_extra structure containing additional context like grouping sets data, active windows, and set operations

## Dependencies
- Functions called/Symbols referenced:
  - standard_qp_extra
  - [RollupData](../R/RollupData.md)
  - [WindowClause](../W/WindowClause.md)
  - linitial_node
  - [grouping_is_sortable](../g/grouping_is_sortable.md)
  - [make_pathkeys_for_sortclauses](../m/make_pathkeys_for_sortclauses.md)
  - [make_pathkeys_for_sortclauses_extended](../m/make_pathkeys_for_sortclauses_extended.md)
  - [adjust_group_pathkeys_for_groupagg](../a/adjust_group_pathkeys_for_groupagg.md)
  - [make_pathkeys_for_window](../m/make_pathkeys_for_window.md)
  - [set_operation_ordered_results_useful](set_operation_ordered_results_useful.md)
  - [generate_setop_child_grouplist](../g/generate_setop_child_grouplist.md)
  - [list_copy](../l/list_copy.md)
- Called from (representative examples):
  - [grouping_planner](../g/grouping_planner.md)
  - standard_qp_extra

## Notes and Other Information
- Grouping sets disable aggregate ordering optimizations and don't combine with aggregate ordering
- [EquivalenceClass](../E/EquivalenceClass.md) processing removes redundant GROUP BY and DISTINCT items (e.g., "WHERE x = y GROUP BY x, y" reduces to "GROUP BY x")
- Only the first window is considered for pathkeys logic in multi-window queries
- Priority ordering ensures the most restrictive/beneficial pathkeys are chosen as query_pathkeys
- The choice between DISTINCT and ORDER BY pathkeys is straightforward since the parser ensures one is a superset of the other
- GROUP BY + ORDER BY interaction requires careful consideration to avoid missing available sort orders
- Set operation pathkeys are only computed when the operation benefits from presorted results