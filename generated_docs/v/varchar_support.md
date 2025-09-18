# varchar_support

## Location
src/backend/utils/adt/varchar.c: 565 - 608

## Overview
Provides planner support for VARCHAR length coercion functions, optimizing cases where length constraints can be simplified or eliminated.

## Definition


## Detailed Description
The `varchar_support` function serves as a planner support function for VARCHAR length coercion operations in PostgreSQL. It is called by the query planner to optimize expressions involving VARCHAR length constraints, particularly when converting between different VARCHAR length specifications.

The main optimization implemented is flattening calls that set a new maximum length that is greater than or equal to the previous maximum length. In such cases, the length coercion becomes redundant and can be simplified to just a type relabeling operation, eliminating unnecessary runtime overhead. The function ignores the `isExplicit` argument since it only affects truncation cases, which are not optimized away.

This function is part of PostgreSQL's planner support infrastructure that allows data types to provide custom optimization logic for their associated functions.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro providing access to:
  - Argument 0: `Node *rawreq` - Support request structure containing optimization context

## Dependencies
- Functions called/Symbols referenced:
  - `SupportRequestSimplify`: Structure type for simplification requests
  - `FuncExpr`: Function expression node type
  - `lsecond`: Macro to get the second element of a list
  - `exprTypmod`: Extracts type modifier from an expression
  - `DatumGetInt32`: Converts Datum to int32 value
  - `relabel_to_typmod`: Creates a relabeling node with new type modifier
  - `VARHDRSZ`: Constant representing variable header size

- Called from (representative examples):
  - PostgreSQL query planner during expression optimization
  - Function call simplification routines

## Notes and Other Information
- This function only handles `SupportRequestSimplify` request types currently
- The optimization eliminates redundant length checks when the new length limit is more permissive than the old one
- Type modifier calculations account for the variable header size (`VARHDRSZ`)
- Returns NULL when no optimization is possible, allowing the original expression to be used
- Part of PostgreSQL's extensible planner support system that allows types to define custom optimizations
- The `isExplicit` parameter is intentionally ignored as noted in the comments
- Registered as a support function in the PostgreSQL system catalogs for VARCHAR type operations