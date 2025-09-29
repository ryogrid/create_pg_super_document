# make_sort_input_target

## Location
[src/backend/optimizer/plan/planner.c:6328-6498](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L6328-L6498)

## Overview
Generates the appropriate PathTarget for initial input to Sort step, determining which expressions should be evaluated before or after sorting for optimal performance.

## Definition
```c
static PathTarget *
make_sort_input_target(PlannerInfo *root,
                       PathTarget *final_target,
                       bool *have_postponed_srfs)
```

## Detailed Description
This function chooses the target to be computed by the node just below the Sort (and DISTINCT, if any) steps when the query has ORDER BY. It implements a sophisticated strategy for deciding whether to evaluate expressions before or after sorting, balancing several competing considerations:

**Postponement Policy:**
- **Volatile expressions**: Always postponed to ensure consistent evaluation order
- **Set-returning functions (SRFs)**: Postponed if none appear in sort columns (to avoid bloating sort dataset and maintain output order)
- **Expensive expressions**: Postponed if there's a LIMIT, partial evaluation is possible, or other expressions are already being postponed

**Constraints:**
- All SRFs in the tlist must be evaluated at the same plan step for synchronized execution in nodeProjectSet
- Grouping/ordering columns cannot be postponed as they're needed for sorting
- Aggref and WindowFunc nodes are preserved since they were computed earlier

The function analyzes each column to determine if it contains SRFs, volatile functions, or expensive operations (>10X cpu_operator_cost), then constructs an appropriate input target.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning information
- `final_target`: The query's final target list in PathTarget form
- `have_postponed_srfs`: Output parameter set to true if any SRFs are postponed to after the Sort

## Dependencies
- Functions called/Symbols referenced:
  - get_pathtarget_sortgroupref
  - [expression_returns_set](../e/expression_returns_set.md)
  - [contain_volatile_functions](../c/contain_volatile_functions.md)
  - [cost_qual_eval_node](../c/cost_qual_eval_node.md)
  - [create_empty_pathtarget](../c/create_empty_pathtarget.md)
  - [add_column_to_pathtarget](../a/add_column_to_pathtarget.md)
  - [pull_var_clause](../p/pull_var_clause.md)
  - [add_new_columns_to_pathtarget](../a/add_new_columns_to_pathtarget.md)
  - [set_pathtarget_cost_width](../s/set_pathtarget_cost_width.md)
  - [list_free](../l/list_free.md)
- Called from:
  - [grouping_planner](../g/grouping_planner.md) (src/backend/optimizer/plan/planner.c:1575)
  - standard_qp_extra (src/backend/optimizer/plan/planner.c:219)

## Notes and Other Information
- This is a static function within planner.c
- Assumes parse->sortClause exists (query has ORDER BY)
- Uses 10X cpu_operator_cost as threshold for "expensive" expressions
- The have_postponed_srfs output affects whether Sort can rely on LIMIT to bound rows
- If no postponement is beneficial, returns final_target unchanged
- Uses PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_WINDOWFUNCS, and PVC_INCLUDE_PLACEHOLDERS flags
- Comments note some redundant cost calculation occurs
- The function handles the case where an explicit Sort might not be used in the final plan

## Simplified Source

```c
static PathTarget *
make_sort_input_target(PlannerInfo *root,
                       PathTarget *final_target,
                       bool *have_postponed_srfs)
{
    Query      *parse = root->parse;
    PathTarget *input_target;
    int         ncols;
    bool       *col_is_srf;
    bool       *postpone_col;
    bool        have_srf = false;
    bool        have_volatile = false;
    bool        have_expensive = false;
    bool        have_srf_sortcols = false;
    bool        postpone_srfs;

    // Must have ORDER BY clause
    Assert(parse->sortClause);
    *have_postponed_srfs = false;

    // Analyze each column in final target
    ncols = list_length(final_target->exprs);
    col_is_srf = palloc0(ncols * sizeof(bool));
    postpone_col = palloc0(ncols * sizeof(bool));

    int i = 0;
    foreach(lc, final_target->exprs) {
        Expr *expr = (Expr *) lfirst(lc);

        // Skip columns needed for sorting/grouping
        if (get_pathtarget_sortgroupref(final_target, i) == 0) {
            // Check for SRFs
            if (parse->hasTargetSRFs && expression_returns_set(expr)) {
                col_is_srf[i] = true;
                have_srf = true;
            }
            // Check for volatile functions - always postpone
            else if (contain_volatile_functions(expr)) {
                postpone_col[i] = true;
                have_volatile = true;
            }
            // Check for expensive functions
            else {
                QualCost cost;
                cost_qual_eval_node(&cost, expr, root);
                if (cost.per_tuple > 10 * cpu_operator_cost) {
                    postpone_col[i] = true;
                    have_expensive = true;
                }
            }
        } else {
            // Check if sort columns contain SRFs
            if (!have_srf_sortcols && parse->hasTargetSRFs &&
                expression_returns_set(expr))
                have_srf_sortcols = true;
        }
        i++;
    }

    // Can postpone SRFs only if none are in sort columns
    postpone_srfs = (have_srf && !have_srf_sortcols);

    // If no postponement needed, return original target
    if (!(postpone_srfs || have_volatile ||
          (have_expensive && (parse->limitCount || root->tuple_fraction > 0))))
        return final_target;

    *have_postponed_srfs = postpone_srfs;

    // Build sort input target with non-postponable columns
    input_target = create_empty_pathtarget();
    List *postponable_cols = NIL;

    i = 0;
    foreach(lc, final_target->exprs) {
        Expr *expr = (Expr *) lfirst(lc);

        if (postpone_col[i] || (postpone_srfs && col_is_srf[i]))
            postponable_cols = lappend(postponable_cols, expr);
        else
            add_column_to_pathtarget(input_target, expr,
                                   get_pathtarget_sortgroupref(final_target, i));
        i++;
    }

    // Add required variables from postponed columns
    List *postponable_vars = pull_var_clause(postponable_cols,
                                            PVC_INCLUDE_AGGREGATES |
                                            PVC_INCLUDE_WINDOWFUNCS |
                                            PVC_INCLUDE_PLACEHOLDERS);
    add_new_columns_to_pathtarget(input_target, postponable_vars);

    return set_pathtarget_cost_width(root, input_target);
}
```