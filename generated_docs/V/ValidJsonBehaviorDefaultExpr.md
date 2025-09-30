# ValidJsonBehaviorDefaultExpr

## Location
[src/backend/parser/parse_expr.c:4663-4695](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L4663-L4695)

## Overview
Recursively validates whether an expression is suitable for use as an ON ERROR or ON EMPTY DEFAULT expression in SQL/JSON functions.

## Definition
```c
static bool ValidJsonBehaviorDefaultExpr(Node *expr, void *context)
```

## Detailed Description
The ValidJsonBehaviorDefaultExpr function performs recursive validation of expressions that are intended to be used as default values in SQL/JSON ON ERROR or ON EMPTY behavior clauses. It implements a whitelist approach, accepting only specific types of expressions that are safe and meaningful as default values. The function accepts simple constants, function calls, and operator expressions directly. For coercion nodes (type conversion expressions), it recursively validates the underlying expressions using expression_tree_walker. This ensures that complex expressions involving type coercions are valid while maintaining security and preventing problematic expressions.

## Parameters / Member Variables
- `expr`: Node pointer to the expression being validated
- `context`: Void pointer for context information (standard walker function parameter, not used in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag
  - expression_tree_walker
  - [ValidJsonBehaviorDefaultExpr](ValidJsonBehaviorDefaultExpr.md) (recursive self-reference)
  - [Node](../N/Node.md) type constants (T_Const, T_FuncExpr, T_OpExpr, T_CoerceViaIO, etc.)
- Called from (representative examples):
  - [transformJsonBehavior](../t/transformJsonBehavior.md)
  - [ValidJsonBehaviorDefaultExpr](ValidJsonBehaviorDefaultExpr.md) (recursive calls)

## Notes and Other Information
This function is part of PostgreSQL's security and validation framework for SQL/JSON functionality. It prevents potentially dangerous or meaningless expressions from being used as default values in JSON behavior clauses. The recursive nature handles complex nested expressions while maintaining strict control over what types of expressions are acceptable. The function is designed to be used with PostgreSQL's expression tree walker infrastructure for comprehensive validation. Located at src/backend/parser/parse_expr.c:4663-4695.

## Simplified Source

```c
static bool
ValidJsonBehaviorDefaultExpr(Node *expr, void *context)
{
    if (expr == NULL)
        return false;

    switch (nodeTag(expr)) {
        // Allow basic expression types
        case T_Const:
        case T_FuncExpr:
        case T_OpExpr:
            return true;

        // Allow coercion nodes if their arguments are valid
        case T_CoerceViaIO:
        case T_CoerceToDomain:
        case T_ArrayCoerceExpr:
        case T_ConvertRowtypeExpr:
        case T_RelabelType:
        case T_CollateExpr:
            return expression_tree_walker(expr, ValidJsonBehaviorDefaultExpr, context);

        default:
            return false;
    }
}
```