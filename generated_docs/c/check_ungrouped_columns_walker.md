# check_ungrouped_columns_walker

## Location
[src/backend/parser/parse_agg.c:1295-1482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_agg.c#L1295-L1482)

## Overview
Core recursive tree walker that implements PostgreSQL's GROUP BY validation logic by examining each node for ungrouped variables and enforcing SQL aggregation rules.

## Definition

```c
static bool
check_ungrouped_columns_walker(Node *node,
							   check_ungrouped_columns_context *context)
```
## Detailed Description
This function implements the sophisticated logic for validating GROUP BY compliance:

1. **Constant and parameter handling**: Immediately accepts constants and parameters as always valid
2. **Aggregate function processing**: 
   - For same-level aggregates, validates direct arguments but allows normal arguments and ORDER BY clauses
   - Skips higher-level aggregates entirely (cannot contain relevant variables)
   - Continues checking lower-level aggregates
3. **GroupingFunc handling**: Properly handles GROUPING() functions at appropriate levels
4. **Complex expression matching**: When non-variable GROUP BY expressions exist, checks if entire subexpressions match GROUP BY items before examining variables within them
5. **Variable validation**:
   - Ignores variables from different query levels
   - Checks simple variable matches against GROUP BY clauses  
   - Performs expensive functional dependency analysis using table constraints as a last resort
   - Maintains func_grouped_rels list to cache functional dependency results
6. **Error generation**: Produces detailed error messages distinguishing between regular ungrouped columns and those in aggregate direct arguments
7. **Recursive traversal**: Handles subquery descent with proper sublevel tracking

The function uses expression_tree_walker and query_tree_walker for efficient tree traversal.

## Parameters / Member Variables
- : Current node being examined in the expression tree
- : Rich context structure containing:

## Dependencies
- Functions called/Symbols referenced:
  - expression_tree_walker
  - query_tree_walker
  - [check_functional_grouping](check_functional_grouping.md)
  - [equal](../e/equal.md)
  - [list_member_int](../l/list_member_int.md)
  - rt_fetch
  - [get_rte_attribute_name](../g/get_rte_attribute_name.md)
  - [lappend_int](../l/lappend_int.md)
- Called from (representative examples):
  - [check_ungrouped_columns](check_ungrouped_columns.md) (entry point)
  - Self-recursion for tree traversal

## Notes and Other Information
- Implements PostgreSQL's sophisticated functional dependency detection using table constraints
- Caches functional dependency results in func_grouped_rels to avoid redundant expensive checks
- Handles aggregate direct arguments specially with detailed error messages for ordered-set aggregates
- Distinguishes between main query ungrouped variables and subquery references to outer variables
- The functional dependency check adds constraints to the query's constraintDeps list for semantic validation
- Part of PostgreSQL's comprehensive SQL standard compliance for GROUP BY semantics

## Simplified Source

```c
static bool
check_ungrouped_columns_walker(Node *node, check_ungrouped_columns_context *context)
{
    if (node == NULL)
        return false;

    // Constants and parameters are always acceptable
    if (IsA(node, Const) || IsA(node, Param))
        return false;

    // Handle aggregate functions at current level
    if (IsA(node, Aggref)) {
        Aggref *agg = (Aggref *) node;

        if ((int) agg->agglevelsup == context->sublevels_up) {
            // Check direct arguments only, skip normal args/ORDER BY/filter
            bool result;
            context->in_agg_direct_args = true;
            result = check_ungrouped_columns_walker((Node *) agg->aggdirectargs, context);
            context->in_agg_direct_args = false;
            return result;
        }

        // Skip higher level aggregates
        if ((int) agg->agglevelsup > context->sublevels_up)
            return false;
    }

    // Handle GroupingFunc
    if (IsA(node, GroupingFunc)) {
        GroupingFunc *grp = (GroupingFunc *) node;
        if ((int) grp->agglevelsup >= context->sublevels_up)
            return false;
    }

    // Check if subexpression matches any GROUP BY item
    if (context->have_non_var_grouping && context->sublevels_up == 0) {
        foreach(gl, context->groupClauses) {
            TargetEntry *tle = lfirst(gl);
            if (equal(node, tle->expr))
                return false; // Found match, acceptable
        }
    }

    // Check for ungrouped variables
    if (IsA(node, Var)) {
        Var *var = (Var *) node;

        if (var->varlevelsup != context->sublevels_up)
            return false; // Not local to this query level

        // Check if variable is in GROUP BY clause
        if (!context->have_non_var_grouping || context->sublevels_up != 0) {
            foreach(gl, context->groupClauses) {
                Var *gvar = (Var *) ((TargetEntry *) lfirst(gl))->expr;
                if (IsA(gvar, Var) &&
                    gvar->varno == var->varno &&
                    gvar->varattno == var->varattno &&
                    gvar->varlevelsup == 0)
                    return false; // Variable is grouped
            }
        }

        // Check functional dependency (expensive check)
        if (list_member_int(*context->func_grouped_rels, var->varno))
            return false; // Previously proven acceptable

        RangeTblEntry *rte = rt_fetch(var->varno, context->pstate->p_rtable);
        if (rte->rtekind == RTE_RELATION) {
            if (check_functional_grouping(rte->relid, var->varno, 0,
                                        context->groupClauseCommonVars,
                                        &context->qry->constraintDeps)) {
                *context->func_grouped_rels =
                    lappend_int(*context->func_grouped_rels, var->varno);
                return false; // Functionally dependent
            }
        }

        // Generate error for ungrouped variable
        char *attname = get_rte_attribute_name(rte, var->varattno);
        if (context->sublevels_up == 0)
            ereport(ERROR, (errcode(ERRCODE_GROUPING_ERROR),
                errmsg("column \"%s.%s\" must appear in the GROUP BY clause or be used in an aggregate function",
                       rte->eref->aliasname, attname)));
        else
            ereport(ERROR, (errcode(ERRCODE_GROUPING_ERROR),
                errmsg("subquery uses ungrouped column \"%s.%s\" from outer query",
                       rte->eref->aliasname, attname)));
    }

    // Handle subqueries
    if (IsA(node, Query)) {
        bool result;
        context->sublevels_up++;
        result = query_tree_walker((Query *) node, check_ungrouped_columns_walker,
                                 (void *) context, 0);
        context->sublevels_up--;
        return result;
    }

    return expression_tree_walker(node, check_ungrouped_columns_walker, (void *) context);
}
```