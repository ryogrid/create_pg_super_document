# transformWhereClause

## Location
[src/backend/parser/parse_clause.c:1854-1880](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L1854-L1880)

## Overview
Transforms SQL qualification expressions (WHERE clauses and similar constructs) into internal expression trees and ensures they evaluate to boolean type.

## Definition

```c
Node *
transformWhereClause(ParseState *pstate, Node *clause,
					 ParseExprKind exprKind, const char *constructName)
```
## Detailed Description
This function is a core component of the PostgreSQL parser responsible for processing WHERE clauses and other qualification expressions in SQL statements. It transforms raw parse tree nodes representing conditional expressions into fully-analyzed internal expression trees. The function performs two critical operations: first, it calls transformExpr to convert the raw clause into a proper expression tree with type information and semantic analysis, and second, it ensures the resulting expression evaluates to a boolean value by calling coerce_to_boolean. This is essential because WHERE clauses must produce true/false results to determine row filtering.

## Parameters / Member Variables
- : The current parsing state containing context information like namespace, query level, etc.
- : The raw parse tree node representing the WHERE clause expression to be transformed
- : An enumeration value specifying the context where the expression appears (affects semantic rules)
- : A descriptive string used in error messages to identify the SQL construct being processed

## Dependencies
- Functions called/Symbols referenced:
  - [transformExpr](transformExpr.md) (expression transformation)
  - [coerce_to_boolean](../c/coerce_to_boolean.md) (type coercion to boolean)
  - [ParseExprKind](../P/ParseExprKind.md) (enumeration type)
- Called from (representative examples):
  - [transformSelectStmt](transformSelectStmt.md)
  - [transformDeleteStmt](transformDeleteStmt.md)
  - [transformUpdateStmt](transformUpdateStmt.md)
  - [transformJoinOnClause](transformJoinOnClause.md)
  - [CreatePolicy](../C/CreatePolicy.md)
  - [CreateTriggerFiringOn](../C/CreateTriggerFiringOn.md)

## Notes and Other Information
- Returns NULL if the input clause is NULL, making WHERE clause optional in many contexts
- The constructName parameter is purely for error reporting and doesn't affect parsing semantics
- This function is widely used across the PostgreSQL parser for processing various types of conditional expressions
- Type coercion to boolean handles implicit conversions (e.g., integers, NULL values) according to PostgreSQL's type system
- The function is declared in parse_clause.h, making it available to other parser modules
- Used in security-related contexts like row-level security policies and triggers