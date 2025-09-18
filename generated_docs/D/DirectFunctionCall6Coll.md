# DirectFunctionCall6Coll

## Location
[src/backend/utils/fmgr/fmgr.c:915-946](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L915-L946)

## Overview
DirectFunctionCall6Coll is a utility function that provides a simplified interface for calling PostgreSQL functions with 6 arguments while specifying a collation, without requiring explicit setup of function call information structures.

## Definition
```c
Datum DirectFunctionCall6Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5, Datum arg6)
```

## Detailed Description
This function serves as a convenience wrapper for calling PostgreSQL internal functions that take exactly 6 arguments and require a specific collation. It automatically handles the setup of the `FunctionCallInfoData` structure, sets the six function arguments, specifies the collation, and performs error checking on the result. The function ensures that NULL results are properly handled by throwing an error if the called function returns NULL, since the caller is clearly not expecting a NULL return value.

The function uses the `LOCAL_FCINFO` macro to create a local function call info structure on the stack, which is more efficient than dynamic allocation for this common use case.

## Parameters / Member Variables
- `func`: A pointer to the PostgreSQL function to be called
- `collation`: The OID of the collation to be used during the function call
- `arg1`: The first argument to pass to the function (as a Datum)
- `arg2`: The second argument to pass to the function (as a Datum)
- `arg3`: The third argument to pass to the function (as a Datum)
- `arg4`: The fourth argument to pass to the function (as a Datum)
- `arg5`: The fifth argument to pass to the function (as a Datum)
- `arg6`: The sixth argument to pass to the function (as a Datum)

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for creating local FunctionCallInfoData)
  - InitFunctionCallInfoData (initializes the function call structure)
  - elog (for error reporting when function returns NULL)

- Called from (representative examples):
  - DirectFunctionCall6 (as a fallback in the macro definition)
  - PG_MODULE_MAGIC (through header inclusion)

## Notes and Other Information
- This function is part of the function manager (fmgr) subsystem in PostgreSQL
- Located in `src/backend/utils/fmgr/fmgr.c:915-946`
- The function automatically sets all six arguments as non-NULL (`isnull = false`)
- Error handling ensures that unexpected NULL returns are caught and reported
- Part of a family of DirectFunctionCall functions that handle different numbers of arguments
- The collation parameter allows for locale-specific operations, particularly important for text processing functions
- This is the highest arity version of DirectFunctionCallColl functions in PostgreSQL
- Less commonly used than lower-arity variants, typically for specialized functions requiring many parameters with collation support