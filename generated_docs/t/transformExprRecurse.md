# transformExprRecurse

## Location
[src/backend/parser/parse_expr.c:138-391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L138-L391)

## Overview
 is the core recursive function that performs the actual transformation of SQL expression trees, dispatching to specialized transformation functions based on node type.

## Definition


## Detailed Description
 serves as the central dispatcher for expression transformation in PostgreSQL's parser. It implements a comprehensive switch statement that handles over 30 different node types, from basic constants and column references to complex JSON expressions and subqueries. The function includes stack overflow protection and transforms raw grammar nodes into fully typed and semantically validated expression trees. Each case delegates to a specialized transformation function that handles the specific semantics of that expression type, ensuring proper type checking, operator resolution, and semantic validation throughout the expression tree.

## Parameters / Member Variables
- : ParseState structure containing current parsing context, including scope information, query structure, and parsing state
- : The raw expression node from the parser that needs to be recursively transformed into a semantic expression tree

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (stack overflow protection)
  - nodeTag (node type identification)
  - transformColumnRef, transformParamRef, transformIndirection (basic expression types)
  - [transformAExprOp](transformAExprOp.md), transformAExprOpAny, transformAExprOpAll (operator expressions)  
  - transformFuncCall, transformSubLink, transformCaseExpr (complex expressions)
  - [transformJsonObjectConstructor](transformJsonObjectConstructor.md), transformJsonArrayConstructor (JSON expressions)
  - [make_const](../m/make_const.md), type_is_rowtype (utility functions)
  - Various AEXPR_* and T_* constants for node type matching

- Called from (representative examples):
  - [transformExpr](transformExpr.md) (main entry point)
  - [transformExprRecurse](transformExprRecurse.md) (recursive self-calls for nested expressions)
  - [transformIndirection](transformIndirection.md), transformAExprOp, transformCaseExpr (specialized transformers)
  - transformFuncCall, transformBoolExpr (for argument processing)

## Notes and Other Information
- Implements comprehensive stack overflow protection via check_stack_depth() to handle deeply nested expressions
- Handles over 30 different PostgreSQL node types in a single switch statement
- Includes special handling for DEFAULT expressions (which should be processed by callers, not passed through)
- Supports both traditional SQL expressions and modern JSON constructor/query expressions introduced in recent PostgreSQL versions
- The function is static, meaning it's only callable from within the parse_expr.c module
- Critical error handling for unrecognized node types to catch parser bugs during development
- Self-recursive design allows for proper transformation of arbitrarily nested expression structures