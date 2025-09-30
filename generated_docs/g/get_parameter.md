# get_parameter

## Location
[src/backend/utils/adt/ruleutils.c:8394-8530](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L8394-L8530)

## Overview
Displays a Param node appropriately by locating and formatting its corresponding expression or generating a suitable textual representation.

## Definition
static void get_parameter(Param *param, deparse_context *context)

## Detailed Description
This function is responsible for converting a Param node into its appropriate textual representation during query deparsing. It employs a multi-step approach to resolve parameters: first attempting to find the referent expression for PARAM_EXEC parameters, then checking for subplan generators, handling PARAM_EXTERN parameters with function argument names, and finally falling back to a simple $N format. The function handles various parameter types including execution parameters, external parameters, and manages proper scoping and qualification of parameter names.

## Parameters / Member Variables
- `param`: The Param node to be displayed
- `context`: Deparse context containing buffer, namespaces, and formatting options

## Dependencies
- Functions called/Symbols referenced:
  - [find_param_referent](../f/find_param_referent.md)
  - [find_param_generator](../f/find_param_generator.md)
  - [push_ancestor_plan](../p/push_ancestor_plan.md)
  - [pop_ancestor_plan](../p/pop_ancestor_plan.md)
  - [get_rule_expr](get_rule_expr.md)
  - [quote_identifier](../q/quote_identifier.md)
  - llast
  - deparse_namespace
  - [SubPlan](../S/SubPlan.md)
  - PARAM_EXTERN
  - [Aggref](../A/Aggref.md)
  - [GroupingFunc](../G/GroupingFunc.md)
- Called from (representative examples):
  - [get_rule_expr](get_rule_expr.md)

## Notes and Other Information
- Handles three main cases: parameter referents, subplan outputs, and external parameters
- For PARAM_EXEC parameters, tries to locate the original expression from ancestor plan nodes
- For subplan outputs, formats as "(subplan_name).colN" notation
- For external parameters, attempts to use function argument names when available
- Applies proper parentheses around complex expressions to maintain atomicity
- Forces variable prefixing when displaying expressions from different plan nodes
- Qualifies parameter names when multiple namespaces exist to avoid ambiguity
- Falls back to simple $N format when other methods fail
- Contains an assertion that non-external parameters should be resolvable

## Simplified Source

```c
static void get_parameter(Param *param, deparse_context *context)
{
    Node *expr;
    deparse_namespace *dpns;
    ListCell *ancestor_cell;
    SubPlan *subplan;
    int column;

    // First, try to find the expression this parameter represents
    expr = find_param_referent(param, context, &dpns, &ancestor_cell);
    if (expr) {
        // Found the original expression - display it
        deparse_namespace save_dpns;
        bool save_varprefix;
        bool need_paren;

        // Switch to ancestor plan context
        push_ancestor_plan(dpns, ancestor_cell, &save_dpns);

        // Force variable prefixing for clarity
        save_varprefix = context->varprefix;
        context->varprefix = true;

        // Add parentheses for complex expressions
        need_paren = !(IsA(expr, Var) || IsA(expr, Aggref) ||
                      IsA(expr, GroupingFunc) || IsA(expr, Param));
        if (need_paren)
            appendStringInfoChar(context->buf, '(');

        get_rule_expr(expr, context, false);

        if (need_paren)
            appendStringInfoChar(context->buf, ')');

        // Restore context
        context->varprefix = save_varprefix;
        pop_ancestor_plan(dpns, &save_dpns);
        return;
    }

    // Check if it's a subplan output parameter
    subplan = find_param_generator(param, context, &column);
    if (subplan) {
        appendStringInfo(context->buf, "(%s%s).col%d",
                        subplan->useHashTable ? "hashed " : "",
                        subplan->plan_name, column + 1);
        return;
    }

    // Try to use function argument names for external parameters
    if (param->paramkind == PARAM_EXTERN && context->namespaces != NIL) {
        dpns = llast(context->namespaces);
        if (dpns->argnames && param->paramid > 0 &&
            param->paramid <= dpns->numargs) {
            char *argname = dpns->argnames[param->paramid - 1];

            if (argname) {
                bool should_qualify = false;
                ListCell *lc;

                // Check if we need to qualify the parameter name
                foreach(lc, context->namespaces) {
                    deparse_namespace *depns = lfirst(lc);
                    if (depns->rtable_names != NIL) {
                        should_qualify = true;
                        break;
                    }
                }

                if (should_qualify) {
                    appendStringInfoString(context->buf,
                                         quote_identifier(dpns->funcname));
                    appendStringInfoChar(context->buf, '.');
                }

                appendStringInfoString(context->buf, quote_identifier(argname));
                return;
            }
        }
    }

    // Default: display as $N
    appendStringInfo(context->buf, "$%d", param->paramid);
}
```