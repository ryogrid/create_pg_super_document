# get_fn_opclass_options

## Location
[src/backend/utils/fmgr/fmgr.c:2097-2144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L2097-L2144)

## Overview
Retrieves operator class options for a support function from PostgreSQL's function manager, returning the options as a bytea structure.

## Definition
```c
bytea *get_fn_opclass_options(FmgrInfo *flinfo)
```

## Detailed Description
This function extracts operator class options from the function's expression context. It validates that the function has proper option data stored as a BYTEA constant in the fn_expr field of the FmgrInfo structure. If valid options are found, it returns a pointer to the bytea data containing the serialized options. If no options are present or the function context is invalid, it throws an error.

The function performs the following operations:
1. Validates the FmgrInfo structure and its fn_expr member
2. Confirms the expression is a Const node of BYTEAOID type
3. Returns the bytea data if present, or NULL if the constant is null
4. Throws an error if options are expected but not found in the calling context

## Parameters / Member Variables
- `flinfo`: Pointer to FmgrInfo structure containing function metadata and expression information that should hold the operator class options

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for node type checking)
  - DatumGetByteaP (converts Datum to bytea pointer)
  - ereport/errmsg/errcode (error reporting functions)
  - ERRCODE_INVALID_PARAMETER_VALUE
  - BYTEAOID (constant for bytea data type)
- Called from (representative examples):
  - PG_GET_OPCLASS_OPTIONS (macro in fmgr.h)
  - OidFunctionCall9 (indirectly via macro)

## Notes and Other Information
- Located in src/backend/utils/fmgr/fmgr.c:2097-2144
- This function is typically called from operator class support functions that need access to their configuration options
- Unlike has_fn_opclass_options(), this function will throw an error if called in an inappropriate context
- The returned bytea pointer should not be freed by the caller as it points to data managed by the expression context
- Used in conjunction with has_fn_opclass_options() to safely check for and retrieve operator class options
- The error message indicates that the function expects to be called only in contexts where operator class options are meaningful