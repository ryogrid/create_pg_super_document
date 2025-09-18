# make_notclause

## Location
src/backend/nodes/makefuncs.c: 733 - 753

## Overview
Creates a NOT boolean expression node to negate a given expression, used in PostgreSQL's query planning and execution system.

## Definition
```c
Expr *make_notclause(Expr *notclause)
```

## Detailed Description
The `make_notclause` function constructs a BoolExpr node representing a NOT operation in PostgreSQL's expression tree. It takes a single expression and creates a boolean expression that evaluates to the logical negation of the input expression. This function is essential for representing negation operations in query optimization and execution.

The function allocates a new BoolExpr node, sets its operation type to NOT_EXPR, and wraps the input expression in a single-element list using `list_make1`. The location is set to -1 to indicate no specific source location. Unlike AND and OR clauses which can have multiple arguments, NOT clauses always have exactly one argument since negation is a unary operation.

## Parameters / Member Variables
- `notclause`: An Expr pointer representing the expression to be logically negated

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create BoolExpr)
  - BoolExpr (expression node type)
  - NOT_EXPR (boolean operation constant)
  - list_make1 (to create single-element list)
- Called from (representative examples):
  - negate_clause (in query optimization)

## Notes and Other Information
- The location field is set to -1, indicating that the clause doesn't correspond to a specific location in the original SQL text
- Unlike `make_andclause` and `make_orclause` which take lists of expressions, this function takes a single expression since NOT is a unary operator
- The expression is wrapped in a list using `list_make1` to maintain consistency with the BoolExpr structure
- Primarily used in query optimization phases, particularly in the `negate_clause` function for logical transformations
- Essential for implementing De Morgan's laws and other logical transformations during query planning
- The returned Expr pointer can be cast back to BoolExpr when needed for further processing