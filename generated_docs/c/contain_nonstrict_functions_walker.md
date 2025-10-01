# contain_nonstrict_functions_walker

## Location
[src/backend/optimizer/util/clauses.c:1005-1136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L1005-L1136)

## Overview
A recursive tree walker function that traverses expression nodes to detect any non-strict constructs that could produce non-NULL output with NULL input.

## Definition
```c
static bool contain_nonstrict_functions_walker(Node *node, void *context)
```

## Detailed Description
This function implements a comprehensive tree walker that recursively examines PostgreSQL expression trees to identify non-strict constructs. It checks various node types including aggregates, window functions, boolean expressions, sublinks, and many others that are inherently non-strict (can return non-NULL values even with NULL inputs). The function is essential for query optimization, particularly for determining whether expressions can be safely pushed down or simplified based on NULL-handling behavior. It uses both explicit node type checking for known non-strict constructs and delegates to `check_functions_in_node` with `contain_nonstrict_functions_checker` for function-specific strictness analysis.

## Parameters / Member Variables
- `node`: The expression node to examine for non-strict constructs
- `context`: A void pointer to context information (currently unused but maintained for walker interface compatibility)

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for node type checking)
  - [getSubscriptingRoutines](../g/getSubscriptingRoutines.md)
  - [check_functions_in_node](check_functions_in_node.md)
  - [contain_nonstrict_functions_checker](contain_nonstrict_functions_checker.md)
  - expression_tree_walker
- Called from (representative examples):
  - [contain_nonstrict_functions](contain_nonstrict_functions.md)
  - [contain_nonstrict_functions_walker](contain_nonstrict_functions_walker.md) (recursive calls)

## Notes and Other Information
- Handles numerous node types including Aggref, GroupingFunc, WindowFunc, BoolExpr, SubLink, CaseExpr, CoalesceExpr, and many others
- Some constructs like AND/OR expressions, aggregates, and CASE expressions are inherently non-strict
- Special handling for CoerceViaIO and ArrayCoerceExpr where strictness depends on specific components
- Uses PostgreSQL's expression_tree_walker infrastructure for comprehensive tree traversal
- Located in src/backend/optimizer/util/clauses.c at lines 1005-1136
- Critical component for NULL-handling optimization in the PostgreSQL query planner

## Simplified Source

```c
static bool contain_nonstrict_functions_walker(Node *node, void *context) {
    if (node == NULL)
        return false;

    // Check for inherently non-strict node types
    if (IsA(node, Aggref) || IsA(node, GroupingFunc) || IsA(node, WindowFunc))
        return true;

    // Array subscripting - assignment is always non-strict
    if (IsA(node, SubscriptingRef)) {
        SubscriptingRef *sbsref = (SubscriptingRef *) node;
        if (sbsref->refassgnexpr != NULL)
            return true;
        // Check if fetch operation is strict
        const SubscriptRoutines *sbsroutines = getSubscriptingRoutines(sbsref->refcontainertype, NULL);
        if (!(sbsroutines && sbsroutines->fetch_strict))
            return true;
    }

    // Boolean expressions - AND/OR are non-strict
    if (IsA(node, BoolExpr)) {
        BoolExpr *expr = (BoolExpr *) node;
        if (expr->boolop == AND_EXPR || expr->boolop == OR_EXPR)
            return true;
    }

    // Other inherently non-strict constructs
    if (IsA(node, DistinctExpr) || IsA(node, NullIfExpr) ||
        IsA(node, SubLink) || IsA(node, SubPlan) || IsA(node, CaseExpr) ||
        IsA(node, CoalesceExpr) || IsA(node, NullTest))
        return true;

    // Special handling for coercion nodes
    if (IsA(node, CoerceViaIO))
        return contain_nonstrict_functions_walker((Node *) ((CoerceViaIO *) node)->arg, context);
    if (IsA(node, ArrayCoerceExpr))
        return contain_nonstrict_functions_walker((Node *) ((ArrayCoerceExpr *) node)->arg, context);

    // Check functions in node and recurse
    if (check_functions_in_node(node, contain_nonstrict_functions_checker, context))
        return true;

    return expression_tree_walker(node, contain_nonstrict_functions_walker, context);
}
```