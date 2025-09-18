# get_fn_expr_argtype

## Location
[src/backend/utils/fmgr/fmgr.c:1910-1928](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1910-L1928)

## Overview
get_fn_expr_argtype extracts the actual type OID of a specific function argument from the function's expression parse tree, enabling polymorphic functions to determine argument types at runtime.

## Definition
```c
Oid get_fn_expr_argtype(FmgrInfo *flinfo, int argnum)
```

## Detailed Description
get_fn_expr_argtype is a utility function that retrieves the actual Object Identifier (OID) of a specific function argument from its expression parse tree. This function is crucial for polymorphic functions that need to determine the types of their arguments at runtime to properly handle type-dependent operations.

The function takes an argument number (zero-based) and examines the fn_expr field of the FmgrInfo structure to extract type information for that specific argument. It delegates the actual type extraction to get_call_expr_argtype(), which handles the parsing of the expression tree. If the FmgrInfo structure is NULL or the fn_expr field has not been initialized, the function returns InvalidOid.

This functionality is essential for PostgreSQL's flexible type system, particularly for aggregate functions, array operations, JSON processing, and other functions that need to adapt their behavior based on the actual argument types provided.

## Parameters / Member Variables
- `flinfo`: Pointer to FmgrInfo structure containing function metadata and expression information
- `argnum`: Zero-based index of the function argument whose type is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [get_call_expr_argtype](get_call_expr_argtype.md) (performs the actual argument type extraction)
  - InvalidOid (constant representing an invalid OID)
- Called from (representative examples):
  - Array aggregate functions (array_agg_transfn, array_agg_array_transfn)
  - JSON processing functions (to_json, json_agg_transfn_worker, jsonb functions)
  - Enum functions (enum_first, enum_last, enum_range functions)
  - Range and multirange functions (range_agg_transfn, multirange_agg_transfn)
  - Text processing functions (concat_internal, text_format)
  - Utility functions (pg_typeof, pg_collation_for, count_nulls)

## Notes and Other Information
- Part of PostgreSQL's polymorphic function support system alongside get_fn_expr_rettype()
- Returns InvalidOid when type information is unavailable or argnum is out of range
- Requires that the fn_expr field in FmgrInfo has been properly initialized by the parser
- Essential for aggregate functions that need to handle different input types
- Heavily used in JSON/JSONB processing for type-aware serialization
- Critical for array functions that need to determine element types
- Used extensively in string formatting and concatenation operations where argument types affect output formatting
- The argnum parameter uses zero-based indexing, consistent with C array conventions