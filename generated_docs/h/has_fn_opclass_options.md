# has_fn_opclass_options

## Location
[src/backend/utils/fmgr/fmgr.c:2081-2096](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L2081-L2096)

## Overview
Checks if options are defined for an operator class support function in PostgreSQL's function manager.

## Definition

```c
bool
has_fn_opclass_options(FmgrInfo *flinfo)
```
## Detailed Description
This function determines whether operator class options are present for a given function by examining the FmgrInfo structure. It specifically checks if the function expression (fn_expr) contains a valid BYTEA constant that represents the operator class options. The function returns true if options are defined and available, false otherwise.

The function performs validation by:
1. Checking if the FmgrInfo pointer and its fn_expr member are valid
2. Verifying that fn_expr is a Const node
3. Confirming the constant is of BYTEAOID type
4. Ensuring the constant is not null

## Parameters / Member Variables
- `*flinfo`: Pointer to FmgrInfo structure containing function metadata and expression information. May be NULL.
## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for node type checking)
  - BYTEAOID (constant for bytea data type)
- Called from (representative examples):
  - PG_HAS_OPCLASS_OPTIONS (macro in fmgr.h)
  - OidFunctionCall9 (indirectly via macro)

## Notes and Other Information
- Located in src/backend/utils/fmgr/fmgr.c:2081-2096
- This is a utility function used in the context of operator class support functions
- The function is safe to call with NULL pointers and will return false in such cases
- Works in conjunction with get_fn_opclass_options() to provide a complete interface for operator class options handling
- The BYTEA type is used to store serialized operator class options data

## Simplified Source
```c
bool has_fn_opclass_options(FmgrInfo *flinfo) {
    // Check if function info exists and has a valid expression
    if (flinfo && flinfo->fn_expr && IsA(flinfo->fn_expr, Const)) {
        Const *expr = (Const *) flinfo->fn_expr;

        // Return true if expression is BYTEA type and not null
        if (expr->consttype == BYTEAOID)
            return !expr->constisnull;
    }

    return false;
}
```