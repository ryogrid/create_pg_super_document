# makeBoolExpr

## Location
src/backend/nodes/makefuncs.c: 418 - 435

## Overview
Creates a BoolExpr node representing boolean expressions such as AND, OR, and NOT operations.

## Definition
```c
Expr *makeBoolExpr(BoolExprType boolop, List *args, int location)
```

## Detailed Description
The `makeBoolExpr` function creates a BoolExpr node that represents boolean expressions in the PostgreSQL parse tree. These expressions handle logical operations like AND, OR, and NOT. The function allocates a new BoolExpr node and initializes it with the specified boolean operation type, argument list, and source location information. This is a fundamental building block for constructing boolean logic in SQL queries.

## Parameters / Member Variables
- `boolop`: The type of boolean operation (AND_EXPR, OR_EXPR, or NOT_EXPR)
- `args`: List of argument expressions that the boolean operation applies to
- `location`: Source location in the original query text for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - BoolExpr (struct type)
  - [BoolExprType](../B/BoolExprType.md) (enum type)
- Called from (representative examples):
  - [transformJoinUsingClause](../t/transformJoinUsingClause.md)
  - transformAExprDistinct
  - transformAExprIn
  - transformAExprBetween
  - [make_row_comparison_op](make_row_comparison_op.md)

## Notes and Other Information
- Used extensively in the parser to convert SQL boolean expressions into internal representation
- The location parameter helps with error reporting by tracking where the expression appeared in the original query
- Arguments list can contain multiple expressions for AND/OR operations, typically one expression for NOT operations
- Essential for query optimization and execution planning of boolean logic