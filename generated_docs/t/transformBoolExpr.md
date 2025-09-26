# transformBoolExpr

## Location
src/backend/parser/parse_expr.c: 1403 - 1438

## Overview
Transforms a Boolean expression node (BoolExpr) during parsing by recursively transforming its operands and applying appropriate type coercion to boolean values.

## Definition


## Detailed Description
The transformBoolExpr function handles the transformation of Boolean expressions during SQL parsing. It processes AND, OR, and NOT expressions by recursively transforming each operand and ensuring all arguments are properly coerced to boolean type. The function maintains the original Boolean operation type and location information while creating a new BoolExpr node with transformed arguments.

The transformation process involves:
1. Identifying the boolean operation type (AND, OR, NOT)
2. Recursively transforming each argument in the expression
3. Coercing each transformed argument to boolean type
4. Creating a new BoolExpr node with the transformed arguments

## Parameters / Member Variables
- : ParseState context containing parsing state information and error handling context
- : BoolExpr node containing the boolean operation type, list of operands, and source location information

## Dependencies
- Functions called/Symbols referenced:
  - BoolExpr (struct type for boolean expressions)
  - AND_EXPR, OR_EXPR, NOT_EXPR (boolean operation type constants)
  - transformExprRecurse (recursively transforms expression nodes)
  - coerce_to_boolean (coerces expressions to boolean type)
  - makeBoolExpr (creates new BoolExpr nodes)
- Called from:
  - transformExprRecurse (main expression transformation dispatcher)

## Notes and Other Information
- This function is part of the SQL parser's expression transformation pipeline
- Handles error cases for unrecognized boolean operations with elog(ERROR)
- Preserves the original location information for error reporting
- All operands are coerced to boolean type regardless of their original type
- The function is static, indicating it's only used within the parse_expr.c module