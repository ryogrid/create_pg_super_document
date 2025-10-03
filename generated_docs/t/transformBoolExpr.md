# transformBoolExpr

## Location
[src/backend/parser/parse_expr.c:1403-1438](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L1403-L1438)

## Overview
Transforms a Boolean expression node (BoolExpr) during parsing by recursively transforming its operands and applying appropriate type coercion to boolean values.

## Definition

```c
static Node *
transformBoolExpr(ParseState *pstate, BoolExpr *a)
```
## Detailed Description
The transformBoolExpr function handles the transformation of Boolean expressions during SQL parsing. It processes AND, OR, and NOT expressions by recursively transforming each operand and ensuring all arguments are properly coerced to boolean type. The function maintains the original Boolean operation type and location information while creating a new BoolExpr node with transformed arguments.

The transformation process involves:
1. Identifying the boolean operation type (AND, OR, NOT)
2. Recursively transforming each argument in the expression
3. Coercing each transformed argument to boolean type
4. Creating a new BoolExpr node with the transformed arguments

## Parameters / Member Variables
- `*pstate`: ParseState context containing parsing state information and error handling context
- `*a`: BoolExpr node containing the boolean operation type, list of operands, and source location information
## Dependencies
- Functions called/Symbols referenced:
  - [BoolExpr](../B/BoolExpr.md) (struct type for boolean expressions)
  - AND_EXPR, OR_EXPR, NOT_EXPR (boolean operation type constants)
  - [transformExprRecurse](transformExprRecurse.md) (recursively transforms expression nodes)
  - [coerce_to_boolean](../c/coerce_to_boolean.md) (coerces expressions to boolean type)
  - [makeBoolExpr](../m/makeBoolExpr.md) (creates new BoolExpr nodes)
- Called from:
  - [transformExprRecurse](transformExprRecurse.md) (main expression transformation dispatcher)

## Notes and Other Information
- This function is part of the SQL parser's expression transformation pipeline
- Handles error cases for unrecognized boolean operations with elog(ERROR)
- Preserves the original location information for error reporting
- All operands are coerced to boolean type regardless of their original type
- The function is static, indicating it's only used within the parse_expr.c module

## Simplified Source

```c
static Node *
transformBoolExpr(ParseState *pstate, BoolExpr *a)
{
    List *args = NIL;
    const char *opname;

    // Determine operation name for error reporting
    switch (a->boolop) {
        case AND_EXPR:
            opname = "AND";
            break;
        case OR_EXPR:
            opname = "OR";
            break;
        case NOT_EXPR:
            opname = "NOT";
            break;
        default:
            elog(ERROR, "unrecognized boolop: %d", (int) a->boolop);
            opname = NULL;
    }

    // Transform and coerce each argument to boolean
    foreach(lc, a->args) {
        Node *arg = lfirst(lc);
        arg = transformExprRecurse(pstate, arg);
        arg = coerce_to_boolean(pstate, arg, opname);
        args = lappend(args, arg);
    }

    return makeBoolExpr(a->boolop, args, a->location);
}
```