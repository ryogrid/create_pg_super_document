# exprIsLengthCoercion

## Location
src/backend/nodes/nodeFuncs.c: 552 - 630

## Overview
Detects whether an expression tree is an application of a datatype's typmod-coercion function and optionally extracts the result's typmod.

## Definition

```c
bool
exprIsLengthCoercion(const Node *expr, int32 *coercedTypmod)
```
## Detailed Description
The  function determines if a given expression represents a length coercion operation - a special type of function that adjusts the type modifier (typically length/precision constraints) of a data type while preserving the base type.

Length coercions are distinguished from type coercions by having specific characteristics:
- For scalar types: FuncExpr with 2-3 arguments where the second argument is an INT4 constant representing the target typmod
- For array types: ArrayCoerceExpr with a non-default (>= 0) resulttypmod

The function validates that the expression came from a coercion context by checking the funcformat field for COERCE_EXPLICIT_CAST or COERCE_IMPLICIT_CAST. Combined type-and-length coercions are also treated as length coercions by this function.

This distinction is important for the type system because length coercions preserve more type information than general type coercions, allowing for better optimization and type inference.

## Parameters / Member Variables
- : A const pointer to the Node representing the expression to be examined
- : Output parameter - if not NULL, receives the target typmod value if this is a length coercion, otherwise receives -1

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for node type checking)
  - list_length (to count function arguments)
  - lsecond (to access second list element)
  - DatumGetInt32 (to extract integer value from Const node)
  - COERCE_EXPLICIT_CAST, COERCE_IMPLICIT_CAST (coercion format constants)

- Called from (representative examples):
  - exprTypmod (to detect length-coercion functions for typmod extraction)
  - get_func_expr (in rule decompilation for proper formatting)
  - QTW_EXAMINE_SORTGROUP (query tree walker examination)

## Notes and Other Information
- Returns false for NULL input expressions
- Sets *coercedTypmod to -1 by default on failure if the parameter is provided
- For FuncExpr: requires exactly 2-3 arguments with second argument being a non-null INT4 constant
- For ArrayCoerceExpr: requires resulttypmod >= 0 (non-default typmod)
- Combined type-and-length coercions are treated as length coercions
- The function is crucial for PostgreSQL's type coercion system and optimization
- Used primarily in type analysis and rule decompilation contexts
- Located in src/backend/nodes/nodeFuncs.c:552-630