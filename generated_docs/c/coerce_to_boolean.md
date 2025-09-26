# coerce_to_boolean

## Location
[src/backend/parser/parse_coerce.c:1161-1207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L1161-L1207)

## Overview
This function coerces an expression to boolean type and validates that it doesn't return a set, used for constructs that require boolean input.

## Definition

```c
Node *
coerce_to_boolean(ParseState *pstate, Node *node,
				  const char *constructName)
```
## Detailed Description
The coerce_to_boolean function is a specialized coercion function designed for SQL constructs that require boolean expressions (AND, OR, NOT, WHERE clauses, etc.). It performs two main validations:

1. **Type Coercion**: If the input expression is not already of boolean type, it attempts to coerce it to boolean using assignment-level coercion rules. If no valid coercion path exists, it reports a clear error message indicating what construct requires the boolean type.

2. **Set Validation**: It checks that the expression does not return a set (multiple values), which is not allowed in boolean contexts. Set-returning functions are prohibited in these contexts.

The function uses COERCION_ASSIGNMENT context, which allows implicit casts that would be valid in assignment operations. If coercion fails, it provides context-specific error messages mentioning the SQL construct name.

## Parameters / Member Variables
- : Parse state for error reporting (can be NULL if special unknown-Param processing is not needed)
- : The input expression node to be coerced to boolean
- : Name of the SQL construct requiring boolean input (for error messages)

## Dependencies
- Functions called/Symbols referenced:
  - [exprType](../e/exprType.md) (get expression type)
  - [coerce_to_target_type](coerce_to_target_type.md) (perform type coercion)
  - [expression_returns_set](../e/expression_returns_set.md) (check for set-returning expressions)
  - [exprLocation](../e/exprLocation.md) (get source location for errors)
  - [format_type_be](../f/format_type_be.md), parser_errposition (error reporting)
  - BOOLOID, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST (constants)
- Called from:
  - [cookConstraint](cookConstraint.md) (constraint processing)
  - [DoCopy](../D/DoCopy.md) (COPY command)
  - [domainAddCheckConstraint](../d/domainAddCheckConstraint.md) (domain constraints)
  - [transformJoinUsingClause](../t/transformJoinUsingClause.md), transformWhereClause (clause processing)
  - [transformAExprIn](../t/transformAExprIn.md), transformBoolExpr, transformCaseExpr (expression transformation)
  - [transformXmlExpr](../t/transformXmlExpr.md), transformBooleanTest (specialized expressions)

## Notes and Other Information
- This is a public function exposed through parse_coerce.h
- Uses COERCION_ASSIGNMENT context, which is more permissive than implicit coercion but stricter than explicit casts
- Provides context-specific error messages that mention the SQL construct name
- The constructName parameter helps users understand where the boolean requirement comes from
- Set-returning functions are specifically prohibited in boolean contexts
- Common use cases include WHERE clauses, JOIN conditions, CASE WHEN conditions, and logical operators