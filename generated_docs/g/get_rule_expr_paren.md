# get_rule_expr_paren

## Location
[src/backend/utils/adt/ruleutils.c:8859-8876](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L8859-L8876)

## Overview
Deparses a PostgreSQL expression node using get_rule_expr, automatically adding parentheses when necessary for pretty printing based on the node's complexity and context.

## Definition

```c
static void
get_rule_expr_paren(Node *node, deparse_context *context,
					bool showimplicit, Node *parentNode)
```
## Detailed Description
This function is a wrapper around get_rule_expr that provides intelligent parentheses management for SQL expression deparsing. It determines whether parentheses are needed by checking if pretty printing with parentheses is enabled and whether the node is "simple" in the context of its parent node. The decision to add parentheses is based on SQL syntax rules and operator precedence to ensure the generated SQL maintains the correct semantic meaning.

The function is designed to be used by parent nodes that do not naturally embrace their child expressions with SQL syntax elements (like parentheses, keywords such as CASE/WHEN/ON, or commas). This ensures that complex expressions maintain proper precedence and readability in the generated SQL output.

## Parameters / Member Variables
- : The expression node to be deparsed
- : Deparse context containing output buffer and formatting settings
- : Whether to show implicit casts and other normally hidden elements
- : The parent node, used to determine if parentheses are needed based on context

## Dependencies
- Functions called/Symbols referenced:
  - PRETTY_PAREN (macro to check if pretty parentheses formatting is enabled)
  - [isSimpleNode](../i/isSimpleNode.md) (determines if a node is simple enough to not need parentheses)
  - [get_rule_expr](get_rule_expr.md) (core expression deparsing function)
  - [appendStringInfoChar](../a/appendStringInfoChar.md) (adds characters to the output buffer)
- Called from (representative examples):
  - [get_rule_expr](get_rule_expr.md) (for various expression types requiring conditional parentheses)
  - [get_oper_expr](get_oper_expr.md) (for operator expressions)
  - [get_func_expr](get_func_expr.md) (for function call expressions)
  - [get_func_sql_syntax](get_func_sql_syntax.md) (for SQL syntax function calls)
  - [get_coercion_expr](get_coercion_expr.md) (for type coercion expressions)

## Notes and Other Information
- This is a static function within ruleutils.c, used extensively throughout expression deparsing
- Parentheses are never added when prettyFlags=0, as the calling node handles them
- The function implements smart parentheses placement based on SQL precedence rules
- Essential for maintaining correct SQL semantics when deparsing complex nested expressions
- Location: src/backend/utils/adt/ruleutils.c:8859-8876

## Simplified Source

```c
static void get_rule_expr_paren(Node *node, deparse_context *context,
                               bool showimplicit, Node *parentNode)
{
    bool need_paren;

    // Determine if parentheses are needed for pretty printing
    need_paren = PRETTY_PAREN(context) &&
                 !isSimpleNode(node, parentNode, context->prettyFlags);

    // Add opening parenthesis if needed
    if (need_paren)
        appendStringInfoChar(context->buf, '(');

    // Deparse the expression
    get_rule_expr(node, context, showimplicit);

    // Add closing parenthesis if needed
    if (need_paren)
        appendStringInfoChar(context->buf, ')');
}
```