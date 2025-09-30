# make_partial_grouping_target

## Location
[src/backend/optimizer/plan/planner.c:5609-5711](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L5609-L5711)

## Overview
Generates the appropriate PathTarget for output of partial aggregate nodes by including grouping columns as-is and converting aggregate functions to partial aggregates with AGGSPLIT_INITIAL_SERIAL mode.

## Definition

```c
structure of the Aggref node,
			 * but flat-copy the node itself to avoid damaging other trees.
			 */
			newaggref = makeNode(Aggref);
```
## Detailed Description
This function creates the target list for partial aggregation nodes in parallel query execution. Partial aggregation is a key optimization technique where aggregation is split into multiple phases - partial aggregation on each worker followed by final aggregation to combine results.

The function handles several critical aspects of partial aggregation:
- Preserves all grouping columns exactly as they appear to enable upper-level grouping
- Converts regular Aggref nodes to partial aggregates marked with AGGSPLIT_INITIAL_SERIAL
- Includes variables and PlaceHolderVars used outside of aggregates in both target list and HAVING clause
- Extracts all aggregates used in HAVING clauses even if not in the main target list
- Ensures comprehensive coverage of variables needed for ORDER BY and window specifications

The transformation is essential for parallel aggregation because partial aggregates produce intermediate results that must be combined by a final aggregation step, rather than producing final aggregate values directly.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and processed grouping information
- : PathTarget representing the tlist to be emitted by the topmost aggregation step
- : Node representing the HAVING clause which may contain additional aggregates and variables

## Dependencies
- Functions called/Symbols referenced:
  - [create_empty_pathtarget](../c/create_empty_pathtarget.md)
  - get_pathtarget_sortgroupref
  - [get_sortgroupref_clause_noerr](../g/get_sortgroupref_clause_noerr.md)
  - [add_column_to_pathtarget](../a/add_column_to_pathtarget.md)
  - [pull_var_clause](../p/pull_var_clause.md)
  - [add_new_columns_to_pathtarget](../a/add_new_columns_to_pathtarget.md)
  - [mark_partial_aggref](mark_partial_aggref.md)
  - [set_pathtarget_cost_width](../s/set_pathtarget_cost_width.md)
- Called from:
  - [create_partial_grouping_paths](../c/create_partial_grouping_paths.md)

## Notes and Other Information
- Uses PVC_INCLUDE_AGGREGATES, PVC_RECURSE_WINDOWFUNCS, and PVC_INCLUDE_PLACEHOLDERS flags for comprehensive expression extraction
- All Aggrefs are converted to partial mode using AGGSPLIT_INITIAL_SERIAL, assuming serialization is required
- Maintains sortgroupref values for grouping columns to preserve their identity across aggregation phases
- Performs flat copying of Aggref nodes to avoid damaging other expression trees
- Essential for two-phase and multi-phase aggregation strategies in parallel query execution
- Results in some redundant cost calculation as noted in the code comment
- Works in conjunction with final aggregation steps that combine partial results into final aggregate values

## Simplified Source

```c
static PathTarget *
make_partial_grouping_target(PlannerInfo *root, PathTarget *grouping_target,
                            Node *havingQual) {
    PathTarget *partial_target;
    List *non_group_cols = NIL;
    List *non_group_exprs;
    int i = 0;
    ListCell *lc;

    partial_target = create_empty_pathtarget();

    // Process each expression in the grouping target
    foreach(lc, grouping_target->exprs) {
        Expr *expr = (Expr *) lfirst(lc);
        Index sgref = get_pathtarget_sortgroupref(grouping_target, i);

        // Check if this is a grouping column
        if (sgref && root->processed_groupClause &&
            get_sortgroupref_clause_noerr(sgref, root->processed_groupClause) != NULL) {
            // Grouping column: add as-is to enable upper-level grouping
            add_column_to_pathtarget(partial_target, expr, sgref);
        } else {
            // Non-grouping column: save for later Var extraction
            non_group_cols = lappend(non_group_cols, expr);
        }
        i++;
    }

    // Include HAVING clause expressions
    if (havingQual)
        non_group_cols = lappend(non_group_cols, havingQual);

    // Extract all Vars, PlaceHolderVars, and Aggrefs from non-grouping expressions
    non_group_exprs = pull_var_clause((Node *) non_group_cols,
                                     PVC_INCLUDE_AGGREGATES |
                                     PVC_RECURSE_WINDOWFUNCS |
                                     PVC_INCLUDE_PLACEHOLDERS);

    add_new_columns_to_pathtarget(partial_target, non_group_exprs);

    // Convert all Aggrefs to partial mode
    foreach(lc, partial_target->exprs) {
        Aggref *aggref = (Aggref *) lfirst(lc);

        if (IsA(aggref, Aggref)) {
            // Flat-copy the Aggref to avoid damaging other trees
            Aggref *newaggref = makeNode(Aggref);
            memcpy(newaggref, aggref, sizeof(Aggref));

            // Mark as partial aggregate with serialization
            mark_partial_aggref(newaggref, AGGSPLIT_INITIAL_SERIAL);

            lfirst(lc) = newaggref;
        }
    }

    // Clean up temporary lists
    list_free(non_group_exprs);
    list_free(non_group_cols);

    // Calculate costs and return
    return set_pathtarget_cost_width(root, partial_target);
}
```