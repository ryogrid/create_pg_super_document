# finalize_grouping_exprs_walker

## Location
[src/backend/parser/parse_agg.c:1502-1656](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_agg.c#L1502-L1656)

## Overview
A tree walker function that finalizes GROUPING expressions by validating their arguments and resolving references to grouping columns, ensuring GROUPING functions only reference valid grouping expressions.

## Definition

```c
static bool
finalize_grouping_exprs_walker(Node *node,
							   check_ungrouped_columns_context *context)
```
## Detailed Description
This function is a recursive tree walker that processes expression trees to finalize GROUPING expressions. It performs several key operations:

1. **Aggregate Handling**: When encountering Aggref nodes at the current query level, it recursively processes only the direct arguments while avoiding normal arguments, ORDER BY arguments, and filters to prevent nested GROUPING expressions.

2. **GROUPING Function Processing**: For GroupingFunc nodes at the appropriate query level, it validates that each argument matches a grouping expression from the current query's GROUP BY clause. It resolves variable references to their corresponding ressortgroupref values and stores these references in the GroupingFunc's refs list.

3. **Query Level Management**: The function correctly handles nested subqueries by adjusting the sublevels_up counter and only processing expressions at the appropriate query level.

4. **Validation**: It enforces the rule that GROUPING function arguments must be exact matches to grouping expressions, rejecting functional dependencies or outer references that would normally be acceptable in other contexts.

## Parameters / Member Variables
- : The expression tree node being processed
- : Context structure containing:
  - : Current nesting level for handling subqueries
  - : List of grouping expressions from the GROUP BY clause
  - : Flag indicating presence of join range table entries
  - : Flag for non-variable grouping expressions
  - : Flag tracking if currently processing aggregate direct arguments
  - : Parse state for error reporting
  - : Query structure for join alias flattening

## Dependencies
- Functions called/Symbols referenced:
  - [flatten_join_alias_vars](flatten_join_alias_vars.md)
  - [equal](../e/equal.md)
  - [exprLocation](../e/exprLocation.md)
  - [lappend_int](../l/lappend_int.md)
  - query_tree_walker
  - expression_tree_walker
  - ereport (for error reporting)
- Called from:
  - [finalize_grouping_exprs](finalize_grouping_exprs.md)
  - Self-recursion for tree traversal

## Notes and Other Information
- This function is part of the PostgreSQL parser's aggregate processing pipeline
- It implements strict validation rules for GROUPING expressions that are more restrictive than normal expression validation
- The function uses the standard PostgreSQL tree walker pattern with context passing
- Error messages provide specific location information for debugging invalid GROUPING usage
- The function handles both variable and non-variable grouping expressions through different code paths

## Simplified Source

```c
static bool
finalize_grouping_exprs_walker(Node *node, check_ungrouped_columns_context *context)
{
    if (node == NULL)
        return false;

    // Constants and parameters are acceptable
    if (IsA(node, Const) || IsA(node, Param))
        return false;

    // Handle aggregate functions
    if (IsA(node, Aggref)) {
        Aggref *agg = (Aggref *) node;

        if ((int) agg->agglevelsup == context->sublevels_up) {
            // Check only direct arguments, not normal args/ORDER BY/filter
            bool result;
            context->in_agg_direct_args = true;
            result = finalize_grouping_exprs_walker((Node *) agg->aggdirectargs, context);
            context->in_agg_direct_args = false;
            return result;
        }

        // Skip higher level aggregates
        if ((int) agg->agglevelsup > context->sublevels_up)
            return false;
    }

    // Process GROUPING functions
    if (IsA(node, GroupingFunc)) {
        GroupingFunc *grp = (GroupingFunc *) node;

        if ((int) grp->agglevelsup == context->sublevels_up) {
            List *ref_list = NIL;

            // Validate each argument matches a grouping expression
            foreach(lc, grp->args) {
                Node *expr = lfirst(lc);
                Index ref = 0;

                if (context->hasJoinRTEs)
                    expr = flatten_join_alias_vars(NULL, context->qry, expr);

                // Check for variable match
                if (IsA(expr, Var)) {
                    Var *var = (Var *) expr;
                    if (var->varlevelsup == context->sublevels_up) {
                        foreach(gl, context->groupClauses) {
                            TargetEntry *tle = lfirst(gl);
                            Var *gvar = (Var *) tle->expr;

                            if (IsA(gvar, Var) &&
                                gvar->varno == var->varno &&
                                gvar->varattno == var->varattno &&
                                gvar->varlevelsup == 0) {
                                ref = tle->ressortgroupref;
                                break;
                            }
                        }
                    }
                }
                // Check for non-variable expression match
                else if (context->have_non_var_grouping && context->sublevels_up == 0) {
                    foreach(gl, context->groupClauses) {
                        TargetEntry *tle = lfirst(gl);
                        if (equal(expr, tle->expr)) {
                            ref = tle->ressortgroupref;
                            break;
                        }
                    }
                }

                // Error if no matching grouping expression found
                if (ref == 0)
                    ereport(ERROR, (errcode(ERRCODE_GROUPING_ERROR),
                        errmsg("arguments to GROUPING must be grouping expressions of the associated query level")));

                ref_list = lappend_int(ref_list, ref);
            }

            grp->refs = ref_list;
        }

        if ((int) grp->agglevelsup > context->sublevels_up)
            return false;
    }

    // Handle subqueries
    if (IsA(node, Query)) {
        bool result;
        context->sublevels_up++;
        result = query_tree_walker((Query *) node, finalize_grouping_exprs_walker,
                                 (void *) context, 0);
        context->sublevels_up--;
        return result;
    }

    return expression_tree_walker(node, finalize_grouping_exprs_walker, (void *) context);
}
```