# get_rule_expr_funccall

## Location
[src/backend/utils/adt/ruleutils.c:10373-10395](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L10373-L10395)

## Overview
Ensures that deparsed expressions look like function calls by wrapping non-function-like expressions in CAST() when necessary.

## Definition

```c
static void
get_rule_expr_funccall(Node *node, deparse_context *context,
					   bool showimplicit)
```
## Detailed Description
 is a specialized wrapper around  that guarantees the output will syntactically resemble a function call or equivalent construct recognized by PostgreSQL's grammar. This function is essential in contexts where the grammar specifically requires a  production and cannot accept a parenthesized .

The function first uses  to determine if the expression will naturally appear as a function-like construct. If so, it delegates to the standard . If not, it wraps the expression in a CAST() operation, which satisfies the grammar requirements and likely reflects what the user originally wrote to produce such a construct.

This mechanism ensures grammatical correctness when reconstructing SQL from internal parse tree representations, particularly in contexts like function arguments or table function calls where function-like syntax is mandatory.

## Parameters / Member Variables
- `*node`: The parse tree node to convert to SQL text
- `*context`: Deparse context containing output buffer, formatting options, and namespace information
- `showimplicit`: Boolean flag controlling whether implicit casts are displayed (set to false for the inner expression when wrapping in CAST)
## Dependencies
- Functions called/Symbols referenced:
  - [looks_like_function](../l/looks_like_function.md)
  - [get_rule_expr](get_rule_expr.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - [format_type_with_typemod](../f/format_type_with_typemod.md)
  - [exprType](../e/exprType.md)
  - [exprTypmod](../e/exprTypmod.md)

- Called from (representative examples):
  - [get_from_clause_item](get_from_clause_item.md) (for function calls in FROM clause)

## Notes and Other Information
- Critical for maintaining grammatical correctness in SQL reconstruction
- The CAST() wrapper technique reflects common user patterns for embedding non-function expressions in function contexts
- Part of PostgreSQL's sophisticated rule deparsing system
- Handles edge cases where expression types don't naturally fit expected grammatical contexts
- Ensures reparseable output that maintains semantic equivalence with original queries

## Simplified Source

```c
static void get_rule_expr_funccall(Node *node, deparse_context *context,
                                  bool showimplicit) {
    if (looks_like_function(node)) {
        // Expression already looks function-like, use standard deparsing
        get_rule_expr(node, context, showimplicit);
    } else {
        // Wrap non-function expressions in CAST() to satisfy grammar
        StringInfo buf = context->buf;

        appendStringInfoString(buf, "CAST(");
        get_rule_expr(node, context, false);  // Don't show implicit casts
        appendStringInfo(buf, " AS %s)",
                        format_type_with_typemod(exprType(node),
                                               exprTypmod(node)));
    }
}
```