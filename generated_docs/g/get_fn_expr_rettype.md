# get_fn_expr_rettype

## Location
src/backend/utils/fmgr/fmgr.c: 1888 - 1909

## Overview
get_fn_expr_rettype extracts the actual return type OID from a function's expression parse tree, primarily used by polymorphic functions to determine their runtime return type.

## Definition
```c
Oid get_fn_expr_rettype(FmgrInfo *flinfo)
```

## Detailed Description
get_fn_expr_rettype is a utility function that retrieves the actual Object Identifier (OID) of a function's return type from its expression parse tree. This function is particularly important for polymorphic functions that can accept multiple input types and need to determine their return type at runtime based on the actual arguments provided.

The function works by examining the fn_expr field of the FmgrInfo structure, which contains the parse tree node representing the function call. It uses the exprType() function to extract the type information from this expression node. If the FmgrInfo structure is NULL or the fn_expr field has not been initialized, the function returns InvalidOid to indicate that type information is not available.

This capability is essential for PostgreSQL's type system, allowing functions to adapt their behavior based on the types they're working with, particularly for generic functions that operate on multiple data types.

## Parameters / Member Variables
- `flinfo`: Pointer to FmgrInfo structure containing function metadata and expression information

## Dependencies
- Functions called/Symbols referenced:
  - exprType (extracts type OID from expression node)
  - InvalidOid (constant representing an invalid OID)
- Called from (representative examples):
  - multirange_constructor functions (for determining appropriate multirange type)
  - range_constructor functions (for determining appropriate range type)  
  - range_agg_finalfn (for range aggregation operations)
  - OidFunctionCall9 (indirect usage through function call infrastructure)

## Notes and Other Information
- Part of PostgreSQL's polymorphic function support system
- Returns InvalidOid when type information is unavailable
- Requires that the fn_expr field in FmgrInfo has been properly initialized by the parser
- Essential for functions that need to determine their return type at runtime
- Used primarily by constructor functions for complex types like ranges and multiranges
- Works in conjunction with get_fn_expr_argtype() for complete type introspection
- The returned OID can be used with PostgreSQL's type system functions for further type operations