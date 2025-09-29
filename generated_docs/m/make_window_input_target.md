# make_window_input_target

## Location
[src/backend/optimizer/plan/planner.c:6081-6200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L6081-L6200)

## Overview
Generates the appropriate PathTarget for initial input to WindowAgg nodes, containing all values needed to evaluate window functions, compute the final target list, and perform any required final sort step.

## Definition

```c
struct a target containing all the non-flattenable targetlist items,
	 * and save aside the others for a moment.
	 */
	input_target = create_empty_pathtarget();
```
## Detailed Description
This function computes the target to be computed by the node just below the first WindowAgg when the query has window functions. The resulting tlist must contain all values needed to evaluate the window functions, compute the final target list, and perform any required final sort step. If multiple WindowAggs are needed, each intermediate one adds its window function results onto this base tlist; only the topmost WindowAgg computes the actual desired target list.

The function is similar to make_group_input_target but with key differences:
- It does not flatten window PARTITION BY/ORDER BY clauses to avoid multiple evaluations
- It preserves GROUP BY clauses that were left unflattened by make_group_input_target
- It does not flatten Aggref expressions since those are computed below the window functions

The algorithm collects sortgroupref numbers from window PARTITION/ORDER BY clauses and GROUP BY clauses, then constructs a target containing non-flattenable items while extracting Vars and Aggrefs from flattenable columns.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning information
- : The query's final target list in PathTarget form
- : List of active windows previously identified by select_active_windows

## Dependencies
- Functions called/Symbols referenced:
  - [create_empty_pathtarget](../c/create_empty_pathtarget.md)
  - get_pathtarget_sortgroupref
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [add_column_to_pathtarget](../a/add_column_to_pathtarget.md)
  - [pull_var_clause](../p/pull_var_clause.md)
  - [add_new_columns_to_pathtarget](../a/add_new_columns_to_pathtarget.md)
  - [set_pathtarget_cost_width](../s/set_pathtarget_cost_width.md)
  - [list_free](../l/list_free.md)
- Called from:
  - [grouping_planner](../g/grouping_planner.md) (src/backend/optimizer/plan/planner.c:1594)
  - standard_qp_extra (src/backend/optimizer/plan/planner.c:214)

## Notes and Other Information
- This is a static function within planner.c
- The function assumes root->parse->hasWindowFuncs is true
- Uses PVC_INCLUDE_AGGREGATES flag to ensure Aggrefs are placed in the Agg node's tlist
- Uses PVC_RECURSE_WINDOWFUNCS to make sure WindowFunc input expressions are available
- The comment notes some redundant cost calculation occurs at the end

## Simplified Source

```c
static PathTarget *
make_window_input_target(PlannerInfo *root,
                         PathTarget *final_target,
                         List *activeWindows)
{
    PathTarget *input_target;
    Bitmapset  *sgrefs;
    List       *flattenable_cols;
    List       *flattenable_vars;

    Assert(root->parse->hasWindowFuncs);

    // Collect sortgroupref numbers from window PARTITION/ORDER BY clauses
    sgrefs = NULL;
    foreach(lc, activeWindows) {
        WindowClause *wc = lfirst_node(WindowClause, lc);

        // Add PARTITION BY clause references
        foreach(lc2, wc->partitionClause) {
            SortGroupClause *sortcl = lfirst_node(SortGroupClause, lc2);
            sgrefs = bms_add_member(sgrefs, sortcl->tleSortGroupRef);
        }

        // Add ORDER BY clause references
        foreach(lc2, wc->orderClause) {
            SortGroupClause *sortcl = lfirst_node(SortGroupClause, lc2);
            sgrefs = bms_add_member(sgrefs, sortcl->tleSortGroupRef);
        }
    }

    // Add GROUP BY clause references
    foreach(lc, root->processed_groupClause) {
        SortGroupClause *grpcl = lfirst_node(SortGroupClause, lc);
        sgrefs = bms_add_member(sgrefs, grpcl->tleSortGroupRef);
    }

    // Build target with non-flattenable items
    input_target = create_empty_pathtarget();
    flattenable_cols = NIL;

    int i = 0;
    foreach(lc, final_target->exprs) {
        Expr *expr = (Expr *) lfirst(lc);
        Index sgref = get_pathtarget_sortgroupref(final_target, i);

        // Keep window/GROUP BY clauses intact - don't flatten
        if (sgref != 0 && bms_is_member(sgref, sgrefs)) {
            add_column_to_pathtarget(input_target, expr, sgref);
        } else {
            // Mark for flattening
            flattenable_cols = lappend(flattenable_cols, expr);
        }
        i++;
    }

    // Extract Vars and Aggrefs from flattenable columns
    // Include aggregates but recurse into WindowFuncs
    flattenable_vars = pull_var_clause(flattenable_cols,
                                       PVC_INCLUDE_AGGREGATES |
                                       PVC_RECURSE_WINDOWFUNCS |
                                       PVC_INCLUDE_PLACEHOLDERS);
    add_new_columns_to_pathtarget(input_target, flattenable_vars);

    return set_pathtarget_cost_width(root, input_target);
}
```