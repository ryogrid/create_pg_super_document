# transformIndirection

## Location
src/backend/parser/parse_expr.c: 438 - 509

## Overview
 transforms PostgreSQL indirection expressions (field access and array subscripting operations like ) into appropriate semantic expression trees.

## Definition


## Detailed Description
 handles the complex process of transforming indirection operations in PostgreSQL, which can involve field selection (accessing columns/fields) and array/container subscripting. The function carefully separates and processes these two types of operations, ensuring that adjacent array indices are treated as a single multidimensional subscript operation while field selections are handled individually. It first transforms the base expression, then processes the indirection chain by distinguishing between A_Indices (array subscripts), A_Star (row expansion - which is not supported in this context), and String nodes (field names). For field selections, it attempts to resolve them via ParseFuncOrColumn and falls back to unknown_attribute for error reporting if resolution fails.

## Parameters / Member Variables
- : ParseState structure containing current parsing context and state information
- : A_Indirection node containing the base expression and list of indirection operations (subscripts and field selections)

## Dependencies
- Functions called/Symbols referenced:
  - [transformExprRecurse](transformExprRecurse.md) (transforms the base expression)
  - [transformContainerSubscripts](transformContainerSubscripts.md) (handles array/container subscripting)
  - [ParseFuncOrColumn](../P/ParseFuncOrColumn.md) (attempts field selection resolution)
  - [unknown_attribute](../u/unknown_attribute.md) (generates appropriate error messages for failed field access)
  - [exprLocation](../e/exprLocation.md), exprType, exprTypmod (expression utility functions)
  - [A_Indices](../A/A_Indices.md), A_Star, String (node type checking)

- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md) (as part of the main expression transformation dispatch)

## Notes and Other Information
- This is a static function, only accessible within the parse_expr.c module
- Implements a two-phase approach: separating subscripting operations from field selections
- Adjacent A_Indices nodes are accumulated and processed together as multidimensional array access
- Row expansion via "*" is explicitly not supported and generates an error
- Field selections are processed via the standard function/column resolution mechanism
- Handles both leading and trailing subscripts correctly in complex indirection chains
- Critical for supporting PostgreSQL's flexible field access syntax including nested composite types and arrays
- The function preserves the p_last_srf context to maintain proper set-returning function tracking during transformation