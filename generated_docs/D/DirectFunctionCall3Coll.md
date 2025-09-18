# DirectFunctionCall3Coll

## Location
src/backend/utils/fmgr/fmgr.c: 834 - 858

## Overview
DirectFunctionCall3Coll is a utility function that provides a simplified interface for calling PostgreSQL functions with 3 arguments while specifying a collation, without requiring explicit setup of function call information structures.

## Definition
```c
Datum DirectFunctionCall3Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3)
```

## Detailed Description
This function serves as a convenience wrapper for calling PostgreSQL internal functions that take exactly 3 arguments and require a specific collation. It automatically handles the setup of the `FunctionCallInfoData` structure, sets the three function arguments, specifies the collation, and performs error checking on the result. The function ensures that NULL results are properly handled by throwing an error if the called function returns NULL, since the caller is clearly not expecting a NULL return value.

The function uses the `LOCAL_FCINFO` macro to create a local function call info structure on the stack, which is more efficient than dynamic allocation for this common use case.

## Parameters / Member Variables
- `func`: A pointer to the PostgreSQL function to be called
- `collation`: The OID of the collation to be used during the function call
- `arg1`: The first argument to pass to the function (as a Datum)
- `arg2`: The second argument to pass to the function (as a Datum)
- `arg3`: The third argument to pass to the function (as a Datum)

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for creating local FunctionCallInfoData)
  - InitFunctionCallInfoData (initializes the function call structure)
  - elog (for error reporting when function returns NULL)

- Called from (representative examples):
  - execute_extension_script (extension script execution - multiple calls)
  - DirectFunctionCall3 (as a fallback in the macro definition)

## Notes and Other Information
- This function is part of the function manager (fmgr) subsystem in PostgreSQL
- Located in `src/backend/utils/fmgr/fmgr.c:834-858`
- The function automatically sets all three arguments as non-NULL (`isnull = false`)
- Error handling ensures that unexpected NULL returns are caught and reported
- Part of a family of DirectFunctionCall functions that handle different numbers of arguments
- The collation parameter allows for locale-specific operations, particularly important for text processing functions
- Commonly used in extension script execution where three-argument functions with collation support are needed