# get_special_variable

## Location
[src/backend/utils/adt/ruleutils.c:7603-7623](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L7603-L7623)

## Overview
A callback function for resolve_special_varno that handles decompilation of special variable references (OUTER_VAR, INNER_VAR, INDEX_VAR) by delegating expression formatting to get_rule_expr.

## Definition

```c
static void
get_special_variable(Node *node, deparse_context *context, void *callback_arg)
```
## Detailed Description
This function serves as a specialized callback for resolve_special_varno when dealing with special variable numbers that don't correspond to regular range table entries. These special variables (OUTER_VAR, INNER_VAR, INDEX_VAR) are used in plan trees to reference expressions from outer relations, inner relations, or index expressions.

The function's primary responsibility is to format the resolved expression node for output. It receives the actual expression that the special variable references (after resolve_special_varno has located the appropriate TargetEntry) and formats it appropriately. The key logic handles non-Var expressions by wrapping them in parentheses to maintain correct precedence when the caller expects a simple variable reference.

## Parameters / Member Variables
- `*node`: The resolved expression node that the special variable actually references
- `*context`: Deparse context containing output buffer and formatting state
- `*callback_arg`: Unused callback argument (reserved for potential future extensions)
## Dependencies
- Functions called/Symbols referenced:
  - [get_rule_expr](get_rule_expr.md) (for recursive expression formatting)
- Called from (representative examples):
  - [get_variable](get_variable.md) (via resolve_special_varno callback mechanism)

## Notes and Other Information
- Designed as a callback function for the resolve_special_varno infrastructure
- Adds parentheses around non-Var expressions to preserve correct operator precedence
- Part of the special variable resolution system that handles OUTER_VAR, INNER_VAR, and INDEX_VAR references in plan trees
- The callback_arg parameter is currently unused but follows the standard callback interface pattern
- Works in conjunction with resolve_special_varno to provide a complete solution for special variable decompilation

## Simplified Source

```c
static void get_special_variable(Node *node, deparse_context *context, void *callback_arg) {
    StringInfo buf = context->buf;

    // Add parentheses for non-Var expressions to maintain precedence
    if (!IsA(node, Var))
        appendStringInfoChar(buf, '(');

    // Format the resolved expression
    get_rule_expr(node, context, true);

    // Close parentheses if we added them
    if (!IsA(node, Var))
        appendStringInfoChar(buf, ')');
}
```