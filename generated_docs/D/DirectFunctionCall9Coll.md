# DirectFunctionCall9Coll

## Location
[src/backend/utils/fmgr/fmgr.c:1017-1064](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1017-L1064)

## Overview
DirectFunctionCall9Coll is a utility function that directly calls a PostgreSQL function with 9 arguments and a specified collation, handling function call setup and result validation.

## Definition
```c
Datum DirectFunctionCall9Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2,
                              Datum arg3, Datum arg4, Datum arg5,
                              Datum arg6, Datum arg7, Datum arg8,
                              Datum arg9)
```

## Detailed Description
This function provides a convenient way to directly invoke PostgreSQL functions that require exactly 9 arguments with collation support. It automatically sets up the function call information structure (FunctionCallInfoData), populates it with the provided arguments (marking all as non-null), executes the function, and validates that the result is not NULL. The function is part of PostgreSQL's function manager (fmgr) system that handles dynamic function calls.

## Parameters / Member Variables
- `func`: Pointer to the PostgreSQL function to be called
- `collation`: Object ID (Oid) specifying the collation to use for the function call
- `arg1`: First argument value of type Datum
- `arg2`: Second argument value of type Datum
- `arg3`: Third argument value of type Datum
- `arg4`: Fourth argument value of type Datum
- `arg5`: Fifth argument value of type Datum
- `arg6`: Sixth argument value of type Datum
- `arg7`: Seventh argument value of type Datum
- `arg8`: Eighth argument value of type Datum
- `arg9`: Ninth argument value of type Datum

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for local function call info allocation)
  - InitFunctionCallInfoData (initializes function call structure)
  - elog (error logging function)
- Called from (representative examples):
  - PG_MODULE_MAGIC (referenced in header)
  - DirectFunctionCall9 (related function without collation)

## Notes and Other Information
- All arguments are automatically marked as non-null (isnull = false)
- The function will throw an ERROR if the called function returns NULL
- This is part of a family of DirectFunctionCallNColl functions for different argument counts
- The collation parameter allows for locale-aware string operations
- Located in src/backend/utils/fmgr/fmgr.c:1017-1064

## Simplified Source

```c
Datum DirectFunctionCall9Coll(PGFunction func, Oid collation,
                              Datum arg1, Datum arg2, Datum arg3,
                              Datum arg4, Datum arg5, Datum arg6,
                              Datum arg7, Datum arg8, Datum arg9) {
    LOCAL_FCINFO(fcinfo, 9);
    Datum result;

    // Initialize function call structure with 9 arguments and collation
    InitFunctionCallInfoData(*fcinfo, NULL, 9, collation, NULL, NULL);

    // Set all nine arguments as non-null
    fcinfo->args[0].value = arg1; fcinfo->args[0].isnull = false;
    fcinfo->args[1].value = arg2; fcinfo->args[1].isnull = false;
    fcinfo->args[2].value = arg3; fcinfo->args[2].isnull = false;
    fcinfo->args[3].value = arg4; fcinfo->args[3].isnull = false;
    fcinfo->args[4].value = arg5; fcinfo->args[4].isnull = false;
    fcinfo->args[5].value = arg6; fcinfo->args[5].isnull = false;
    fcinfo->args[6].value = arg7; fcinfo->args[6].isnull = false;
    fcinfo->args[7].value = arg8; fcinfo->args[7].isnull = false;
    fcinfo->args[8].value = arg9; fcinfo->args[8].isnull = false;

    // Call the function
    result = (*func)(fcinfo);

    // Ensure function didn't return NULL unexpectedly
    if (fcinfo->isnull)
        elog(ERROR, "function %p returned NULL", (void *) func);

    return result;
}
```