# FunctionCall7Coll

## Location
src/backend/utils/fmgr/fmgr.c: 1284 - 1317

## Overview
FunctionCall7Coll is a PostgreSQL function manager utility that invokes a database function with 7 arguments and a specified collation, ensuring a non-null result is returned.

## Definition
```c
Datum FunctionCall7Coll(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5, Datum arg6, Datum arg7)
```

## Detailed Description
FunctionCall7Coll is part of PostgreSQL's function manager (fmgr) system that provides a standardized interface for calling database functions. This function specifically handles the invocation of functions that require exactly 7 arguments along with collation information. It follows the established pattern of the FunctionCallNColl family: sets up the function call context, initializes all arguments as non-null, invokes the target function, and validates that the result is not null. If the called function returns NULL, an error is raised since the caller explicitly expects a non-null result.

The function uses the LOCAL_FCINFO macro to create a local FunctionCallInfo structure on the stack for efficient temporary function calls. This function variant is commonly used in GIN (Generalized Inverted Index) operations and selectivity estimation functions where complex operations require multiple parameters.

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

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for creating local FunctionCallInfo)
  - InitFunctionCallInfoData (initializes function call context)
  - FunctionCallInvoke (performs the actual function invocation)
- Called from (representative examples):
  - directTriConsistentFn (GIN direct tri-consistent function handling)
  - shimBoolConsistentFn (GIN boolean consistent function shimming)
  - ginNewScanKey (GIN index scan key creation)
  - gincost_pattern (GIN cost estimation for pattern matching)
  - OidFunctionCall7Coll (OID-based function calling wrapper)

## Notes and Other Information
- This function is part of a family of FunctionCallNColl functions (where N ranges from 0 to 9) that handle different numbers of arguments
- The function enforces non-null return values by raising an ERROR if the called function returns NULL
- All input arguments are automatically marked as non-null in the function call context
- The LOCAL_FCINFO macro provides stack-based allocation for better performance in frequently called scenarios
- Collation support is essential for proper string comparison and sorting operations in multi-language databases
- This 7-argument variant is particularly used in GIN index operations where complex consistency checking requires multiple parameters
- Located in src/backend/utils/fmgr/fmgr.c:1284-1317