# exprLocation

## Location
[src/backend/nodes/nodeFuncs.c:1380-1809](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L1380-L1809)

## Overview
Returns the parse location of an expression tree node for error reporting purposes, finding the leftmost position in complex expressions to provide meaningful error context.

## Definition


## Detailed Description
The  function determines and returns the parse location (character position in the original SQL text) of an expression tree node. This information is crucial for generating accurate error messages that point to the correct location in the user's SQL query.

The function implements sophisticated logic to find the most meaningful location for complex expressions. For simple node types like constants and variables, it returns their stored location directly. For complex expressions like operators and function calls, it attempts to find the leftmost token that represents the start of the entire expression, not just the topmost node.

Key design principles:
- For expressions larger than a single token, returns the leftmost token location
- For operators, considers both the operator location and operand locations to find the true start
- Handles implicit nodes (created by parse analysis) that may have location -1
- Recursively processes nested expressions to find valid locations
- Returns -1 if the location cannot be determined

The function handles over 50 different node types, from simple literals to complex constructs like window functions, JSON expressions, and partition specifications.

## Parameters
- : The expression tree node whose location is to be determined (const Node pointer, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (to determine expression type)
  - [leftmostLoc](../l/leftmostLoc.md) (helper function to find leftmost of two locations)
  - Recursive calls to exprLocation for nested expressions
  - Various node type constants (T_Var, T_Const, T_FuncExpr, etc.)

- Called from (representative examples):
  - Various parser functions for error reporting (parse_clause.c, parse_expr.c, etc.)
  - Expression transformation functions
  - Error handling routines throughout the parser
  - [Query](../Q/Query.md) analysis functions

## Notes and Other Information
- Returns -1 if the location cannot be determined or if expr is NULL
- The location may not be perfect due to grammar limitations (e.g., parentheses aren't explicitly represented)
- Handles both raw parse tree nodes (A_Expr, ColumnRef) and processed expression nodes (OpExpr, FuncExpr)
- Some expressions created by parse analysis may have location -1, requiring fallback to operand locations
- For lists, reports the location of the first list member that has a valid location
- Special handling for different expression types based on their structure and semantics
- Critical for PostgreSQL's error reporting system, providing user-friendly error locations
- Located in src/backend/nodes/nodeFuncs.c:1380-1809