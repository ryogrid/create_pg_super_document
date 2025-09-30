# adjust_group_pathkeys_for_groupagg

## Location
[src/backend/optimizer/plan/planner.c:3285-3508](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L3285-L3508)

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
  - [Aggref](../A/Aggref.md)
  - AGGKIND_IS_ORDERED_SET
  - [RelabelType](../R/RelabelType.md)
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

## Simplified Source

```c
static void
adjust_group_pathkeys_for_groupagg(PlannerInfo *root)
{
    List *grouppathkeys = root->group_pathkeys;
    List *bestpathkeys = NIL;
    Bitmapset *bestaggs = NULL;
    Bitmapset *unprocessed_aggs = NULL;
    ListCell *lc;
    int i;

    // Early exits
    Assert(root->parse->groupingSets == NIL);
    Assert(root->numOrderedAggs > 0);
    if (!enable_presorted_aggregate)
        return;

    // Phase 1: Collect processable aggregates
    foreach(lc, root->agginfos)
    {
        AggInfo *agginfo = lfirst_node(AggInfo, lc);
        Aggref *aggref = linitial_node(Aggref, agginfo->aggrefs);

        // Skip ordered sets and aggregates without ORDER BY/DISTINCT
        if (AGGKIND_IS_ORDERED_SET(aggref->aggkind) ||
            (aggref->aggdistinct == NIL && aggref->aggorder == NIL))
            continue;

        // Safety check for filtered aggregates
        if (aggref->aggfilter != NULL)
        {
            ListCell *lc2;
            bool allow_presort = true;

            // Only allow simple expressions (Vars/Consts) in filtered aggregates
            foreach(lc2, aggref->args)
            {
                TargetEntry *tle = (TargetEntry *) lfirst(lc2);
                Expr *expr = tle->expr;

                // Strip RelabelType wrappers
                while (IsA(expr, RelabelType))
                    expr = (Expr *) (castNode(RelabelType, expr))->arg;

                if (!IsA(expr, Var) && !IsA(expr, Const))
                {
                    allow_presort = false;
                    break;
                }
            }

            if (!allow_presort)
                continue;
        }

        unprocessed_aggs = bms_add_member(unprocessed_aggs, foreach_current_index(lc));
    }

    // Phase 2: Find best pathkeys iteratively
    while (bms_num_members(unprocessed_aggs) > bms_num_members(bestaggs))
    {
        Bitmapset *aggindexes = NULL;
        List *currpathkeys = NIL;

        // Process each unprocessed aggregate
        i = -1;
        while ((i = bms_next_member(unprocessed_aggs, i)) >= 0)
        {
            AggInfo *agginfo = list_nth_node(AggInfo, root->agginfos, i);
            Aggref *aggref = linitial_node(Aggref, agginfo->aggrefs);
            List *sortlist = (aggref->aggdistinct != NIL) ?
                           aggref->aggdistinct : aggref->aggorder;
            List *pathkeys = make_pathkeys_for_sortclauses(root, sortlist, aggref->args);

            // Skip aggregates with volatile functions
            if (has_volatile_pathkey(pathkeys))
            {
                unprocessed_aggs = bms_del_member(unprocessed_aggs, i);
                continue;
            }

            // Initialize or compare pathkeys
            if (currpathkeys == NIL)
            {
                // First aggregate - set base pathkeys
                currpathkeys = pathkeys;
                if (grouppathkeys != NIL)
                    currpathkeys = append_pathkeys(list_copy(grouppathkeys), currpathkeys);
                aggindexes = bms_add_member(aggindexes, i);
            }
            else
            {
                // Check compatibility with current pathkeys
                if (grouppathkeys != NIL)
                    pathkeys = append_pathkeys(list_copy(grouppathkeys), pathkeys);

                switch (compare_pathkeys(currpathkeys, pathkeys))
                {
                    case PATHKEYS_BETTER2:
                        // New pathkeys are stronger - use them
                        currpathkeys = pathkeys;
                        /* FALLTHROUGH */
                    case PATHKEYS_BETTER1:
                    case PATHKEYS_EQUAL:
                        // Compatible - include this aggregate
                        aggindexes = bms_add_member(aggindexes, i);
                        break;
                    case PATHKEYS_DIFFERENT:
                        // Incompatible - skip
                        break;
                }
            }
        }

        // Update processed aggregates
        unprocessed_aggs = bms_del_members(unprocessed_aggs, aggindexes);

        // Update best solution if this one is better
        if (bms_num_members(aggindexes) > bms_num_members(bestaggs))
        {
            bestaggs = aggindexes;
            bestpathkeys = currpathkeys;
        }
    }

    // Phase 3: Apply results
    if (bestpathkeys != NIL)
        root->group_pathkeys = bestpathkeys;

    // Mark compatible aggregates as presorted
    i = -1;
    while ((i = bms_next_member(bestaggs, i)) >= 0)
    {
        AggInfo *agginfo = list_nth_node(AggInfo, root->agginfos, i);
        foreach(lc, agginfo->aggrefs)
        {
            Aggref *aggref = lfirst_node(Aggref, lc);
            aggref->aggpresorted = true;
        }
    }
}
```