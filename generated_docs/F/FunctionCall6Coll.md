# FunctionCall6Coll

## Location
[src/backend/utils/fmgr/fmgr.c:1252-1283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1252-L1283)

## Overview
FunctionCall6Coll is a PostgreSQL function manager utility that invokes a database function with 6 arguments and a specified collation, ensuring a non-null result is returned.

## Definition
```c
Datum FunctionCall6Coll(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5, Datum arg6)
```

## Detailed Description
FunctionCall6Coll is part of PostgreSQL's function manager (fmgr) system that provides a standardized interface for calling database functions. This function specifically handles the invocation of functions that require exactly 6 arguments along with collation information. It follows the same pattern as other FunctionCallNColl functions: sets up the function call context, initializes all arguments as non-null, invokes the target function, and validates that the result is not null. If the called function returns NULL, an error is raised since the caller explicitly expects a non-null result.

The function uses the LOCAL_FCINFO macro to create a local FunctionCallInfo structure on the stack for efficient temporary function calls. This is consistent with PostgreSQL's optimization strategy for frequent function invocations throughout the system.

## Parameters / Member Variables
- `flinfo`: Pointer to FmgrInfo structure containing metadata about the function to be called (function OID, argument info, etc.)
- `collation`: OID specifying the collation to use for string comparisons and operations within the called function
- `arg1`: First argument to pass to the target function (Datum type - PostgreSQL's generic data container)
- `arg2`: Second argument to pass to the target function
- `arg3`: Third argument to pass to the target function
- `arg4`: Fourth argument to pass to the target function
- `arg5`: Fifth argument to pass to the target function
- `arg6`: Sixth argument to pass to the target function

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for creating local FunctionCallInfo)
  - InitFunctionCallInfoData (initializes function call context)
  - FunctionCallInvoke (performs the actual function invocation)
- Called from (representative examples):
  - [OidFunctionCall6Coll](../O/OidFunctionCall6Coll.md) (OID-based function calling wrapper)
  - FunctionCall6 (non-collation variant wrapper)

## Notes and Other Information
- This function is part of a family of FunctionCallNColl functions (where N ranges from 0 to 9) that handle different numbers of arguments
- The function enforces non-null return values by raising an ERROR if the called function returns NULL
- All input arguments are automatically marked as non-null in the function call context
- The LOCAL_FCINFO macro provides stack-based allocation for better performance in frequently called scenarios
- Collation support is essential for proper string comparison and sorting operations in multi-language databases
- This variant with 6 arguments is less commonly used compared to functions with fewer parameters
- Located in src/backend/utils/fmgr/fmgr.c:1252-1283

## Simplified Source
```c
Datum FunctionCall6Coll(FmgrInfo *flinfo, Oid collation,
                        Datum arg1, Datum arg2, Datum arg3,
                        Datum arg4, Datum arg5, Datum arg6) {
    LOCAL_FCINFO(fcinfo, 6);

    // Initialize function call context with 6 arguments and collation
    InitFunctionCallInfoData(*fcinfo, flinfo, 6, collation, NULL, NULL);

    // Set all 6 arguments as non-null values
    fcinfo->args[0].value = arg1; fcinfo->args[0].isnull = false;
    fcinfo->args[1].value = arg2; fcinfo->args[1].isnull = false;
    fcinfo->args[2].value = arg3; fcinfo->args[2].isnull = false;
    fcinfo->args[3].value = arg4; fcinfo->args[3].isnull = false;
    fcinfo->args[4].value = arg5; fcinfo->args[4].isnull = false;
    fcinfo->args[5].value = arg6; fcinfo->args[5].isnull = false;

    // Invoke the target function
    Datum result = FunctionCallInvoke(fcinfo);

    // Ensure non-null result (error if NULL returned)
    if (fcinfo->isnull)
        elog(ERROR, "function %u returned NULL", flinfo->fn_oid);

    return result;
}
```