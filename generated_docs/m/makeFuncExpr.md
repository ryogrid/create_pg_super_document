# makeFuncExpr

## Location
src/backend/nodes/makefuncs.c: 568 - 591

## Overview
Constructs a FuncExpr node representing a function call expression in the query tree, with all necessary metadata for function execution.

## Definition
```c
FuncExpr *makeFuncExpr(Oid funcid, Oid rettype, List *args, Oid funccollid, Oid inputcollid, CoercionForm fformat)
```

## Detailed Description
The `makeFuncExpr` function creates a FuncExpr node that represents a function call in PostgreSQL's expression tree structure. This function is used to build expression nodes for both built-in and user-defined functions during query planning and execution. The function requires that all argument expressions have already been transformed before being passed to this constructor.

The resulting FuncExpr contains all the metadata needed for proper function resolution and execution, including collation information and coercion formatting. It assumes the function is not set-returning and not variadic, which covers the most common function call scenarios.

## Parameters / Member Variables
- `funcid`: The object identifier (OID) of the function to be called
- `rettype`: The OID of the function's return type
- `args`: A list of already-transformed argument expressions for the function call
- `funccollid`: The collation OID to use for the function call result
- `inputcollid`: The collation OID to use for input argument processing
- `fformat`: The coercion format specifying how type coercion should be applied

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for FuncExpr allocation)
  - FuncExpr (struct type)
  - CoercionForm (enum type)
- Called from (representative examples):
  - build_aggregate_transfn_expr
  - build_aggregate_finalfn_expr
  - build_coercion_expression
  - makeJsonByteaToTextConversion
  - transformJsonValueExpr
  - coerceJsonFuncExpr
  - get_qual_for_hash
  - make_path_cat_expr
  - rewriteSearchAndCycle

## Notes and Other Information
- Sets funcretset to false (assumes non-set-returning function)
- Sets funcvariadic to false (assumes non-variadic function)
- Sets location to -1 (unknown source location)
- Requires pre-transformed argument expressions
- Used extensively in expression building, type coercion, and aggregate processing
- Critical component in query plan generation and execution
- Declared in src/include/nodes/makefuncs.h at line 79
- Commonly used in parser transformations, coercion operations, and specialized query rewriting scenarios