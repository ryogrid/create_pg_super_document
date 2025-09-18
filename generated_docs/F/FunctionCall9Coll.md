# FunctionCall9Coll

## Location
[src/backend/utils/fmgr/fmgr.c:1354-1400](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1354-L1400)

## Overview
FunctionCall9Coll is a PostgreSQL function manager utility that invokes a database function with 9 arguments and a specified collation, ensuring a non-null result is returned.

## Definition
```c
Datum FunctionCall9Coll(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5, Datum arg6, Datum arg7, Datum arg8, Datum arg9)
```

## Detailed Description
FunctionCall9Coll is part of PostgreSQL's function manager (fmgr) system that provides a standardized interface for calling database functions. This function specifically handles the invocation of functions that require exactly 9 arguments along with collation information. It follows the established pattern of the FunctionCallNColl family: sets up the function call context, initializes all arguments as non-null, invokes the target function, and validates that the result is not null. If the called function returns NULL, an error is raised since the caller explicitly expects a non-null result.

The function uses the LOCAL_FCINFO macro to create a local FunctionCallInfo structure on the stack for efficient temporary function calls. This variant with 9 arguments represents the highest-arity function call interface in the FunctionCallNColl family and is used in very specialized operations requiring many parameters.

## Parameters / Member Variables
- `flinfo`: Pointer to FmgrInfo structure containing metadata about the function to be called (function OID, argument info, etc.)
- `collation`: OID specifying the collation to use for string comparisons and operations within the called function
- `arg1`: First argument to pass to the target function (Datum type - PostgreSQL's generic data container)
- `arg2`: Second argument to pass to the target function
- `arg3`: Third argument to pass to the target function
- `arg4`: Fourth argument to pass to the target function
- `arg5`: Fifth argument to pass to the target function
- `arg6`: Sixth argument to pass to the target function
- `arg7`: Seventh argument to pass to the target function
- `arg8`: Eighth argument to pass to the target function
- `arg9`: Ninth argument to pass to the target function

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for creating local FunctionCallInfo)
  - InitFunctionCallInfoData (initializes function call context)
  - FunctionCallInvoke (performs the actual function invocation)
- Called from (representative examples):
  - [OidFunctionCall9Coll](../O/OidFunctionCall9Coll.md) (OID-based function calling wrapper)
  - FunctionCall9 (non-collation variant wrapper)

## Notes and Other Information
- This function is part of a family of FunctionCallNColl functions (where N ranges from 0 to 9) that handle different numbers of arguments
- The function enforces non-null return values by raising an ERROR if the called function returns NULL
- All input arguments are automatically marked as non-null in the function call context
- The LOCAL_FCINFO macro provides stack-based allocation for better performance in frequently called scenarios
- Collation support is essential for proper string comparison and sorting operations in multi-language databases
- This 9-argument variant represents the highest-arity function call interface and is rarely used, reserved for the most complex operations requiring many parameters
- Limited usage compared to lower-arity variants, primarily available for specialized or extensibility purposes
- Located in src/backend/utils/fmgr/fmgr.c:1354-1400