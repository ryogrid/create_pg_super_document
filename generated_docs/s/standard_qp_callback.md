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

## Simplified Source

```c
static void standard_qp_callback(PlannerInfo *root, void *extra)
{
    Query *parse = root->parse;
    standard_qp_extra *qp_extra = (standard_qp_extra *) extra;
    List *tlist = root->processed_tlist;
    List *activeWindows = qp_extra->activeWindows;

    // Compute group pathkeys
    if (qp_extra->gset_data) {
        // With grouping sets, use first RollupData's groupClause
        List *rollups = qp_extra->gset_data->rollups;
        List *groupClause = (rollups ? linitial_node(RollupData, rollups)->groupClause : NIL);

        if (grouping_is_sortable(groupClause)) {
            root->group_pathkeys = make_pathkeys_for_sortclauses(root,
                                                               groupClause,
                                                               tlist);
            root->num_groupby_pathkeys = list_length(root->group_pathkeys);
        } else {
            root->group_pathkeys = NIL;
            root->num_groupby_pathkeys = 0;
        }
    }
    else if (parse->groupClause || root->numOrderedAggs > 0) {
        // Plain GROUP BY: remove redundant items via EquivalenceClass processing
        bool sortable;

        root->group_pathkeys =
            make_pathkeys_for_sortclauses_extended(root,
                                                 &root->processed_groupClause,
                                                 tlist, true, &sortable, true);
        if (!sortable) {
            root->group_pathkeys = NIL;
            root->num_groupby_pathkeys = 0;
        } else {
            root->num_groupby_pathkeys = list_length(root->group_pathkeys);
            // Add aggregate ordering if present
            if (root->numOrderedAggs > 0)
                adjust_group_pathkeys_for_groupagg(root);
        }
    } else {
        root->group_pathkeys = NIL;
        root->num_groupby_pathkeys = 0;
    }

    // Compute window pathkeys (only first window considered)
    if (activeWindows != NIL) {
        WindowClause *wc = linitial_node(WindowClause, activeWindows);
        root->window_pathkeys = make_pathkeys_for_window(root, wc, tlist);
    } else {
        root->window_pathkeys = NIL;
    }

    // Compute distinct pathkeys
    if (parse->distinctClause) {
        bool sortable;
        root->processed_distinctClause = list_copy(parse->distinctClause);
        root->distinct_pathkeys =
            make_pathkeys_for_sortclauses_extended(root,
                                                 &root->processed_distinctClause,
                                                 tlist, true, &sortable, false);
        if (!sortable)
            root->distinct_pathkeys = NIL;
    } else {
        root->distinct_pathkeys = NIL;
    }

    // Compute sort pathkeys
    root->sort_pathkeys = make_pathkeys_for_sortclauses(root,
                                                       parse->sortClause,
                                                       tlist);

    // Compute set operation pathkeys if useful
    if (qp_extra->setop != NULL &&
        set_operation_ordered_results_useful(qp_extra->setop)) {
        List *groupClauses;
        bool sortable;

        groupClauses = generate_setop_child_grouplist(qp_extra->setop, tlist);
        root->setop_pathkeys =
            make_pathkeys_for_sortclauses_extended(root, &groupClauses,
                                                 tlist, false, &sortable, false);
        if (!sortable)
            root->setop_pathkeys = NIL;
    } else {
        root->setop_pathkeys = NIL;
    }

    // Choose query_pathkeys in priority order
    if (root->group_pathkeys)
        root->query_pathkeys = root->group_pathkeys;
    else if (root->window_pathkeys)
        root->query_pathkeys = root->window_pathkeys;
    else if (list_length(root->distinct_pathkeys) >
             list_length(root->sort_pathkeys))
        root->query_pathkeys = root->distinct_pathkeys;
    else if (root->sort_pathkeys)
        root->query_pathkeys = root->sort_pathkeys;
    else if (root->setop_pathkeys != NIL)
        root->query_pathkeys = root->setop_pathkeys;
    else
        root->query_pathkeys = NIL;
}
```