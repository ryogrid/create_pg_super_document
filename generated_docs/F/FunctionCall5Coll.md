# FunctionCall5Coll

## Location
[src/backend/utils/fmgr/fmgr.c:1223-1251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1223-L1251)

## Overview
FunctionCall5Coll is a PostgreSQL function manager utility that invokes a database function with 5 arguments and a specified collation, ensuring a non-null result is returned.

## Definition
```c
Datum FunctionCall5Coll(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5)
```

## Detailed Description
FunctionCall5Coll is part of PostgreSQL's function manager (fmgr) system that provides a standardized interface for calling database functions. This function specifically handles the invocation of functions that require exactly 5 arguments along with collation information. It sets up the function call context, initializes all arguments as non-null, invokes the target function, and validates that the result is not null. If the called function returns NULL, an error is raised since the caller explicitly expects a non-null result.

The function uses the LOCAL_FCINFO macro to create a local FunctionCallInfo structure on the stack, which is more efficient than heap allocation for temporary function calls. This is part of PostgreSQL's optimization strategy for frequent function invocations.

## Parameters / Member Variables
- `flinfo`: Pointer to FmgrInfo structure containing metadata about the function to be called (function OID, argument info, etc.)
- `collation`: OID specifying the collation to use for string comparisons and operations within the called function
- `arg1`: First argument to pass to the target function (Datum type - PostgreSQL's generic data container)
- `arg2`: Second argument to pass to the target function
- `arg3`: Third argument to pass to the target function
- `arg4`: Fourth argument to pass to the target function
- `arg5`: Fifth argument to pass to the target function

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for creating local FunctionCallInfo)
  - InitFunctionCallInfoData (initializes function call context)
  - FunctionCallInvoke (performs the actual function invocation)
- Called from (representative examples):
  - [gistindex_keytest](../g/gistindex_keytest.md) (GiST index key testing)
  - [update_frameheadpos](../u/update_frameheadpos.md) (window aggregate frame position updates)
  - [update_frametailpos](../u/update_frametailpos.md) (window aggregate frame position updates)
  - scalararraysel (selectivity estimation for scalar array operations)
  - [OidFunctionCall5Coll](../O/OidFunctionCall5Coll.md) (OID-based function calling wrapper)

## Notes and Other Information
- This function is part of a family of FunctionCallNColl functions (where N ranges from 0 to 9) that handle different numbers of arguments
- The function enforces non-null return values by raising an ERROR if the called function returns NULL
- All input arguments are automatically marked as non-null in the function call context
- The LOCAL_FCINFO macro provides stack-based allocation for better performance in frequently called scenarios
- Collation support is essential for proper string comparison and sorting operations in multi-language databases
- Located in src/backend/utils/fmgr/fmgr.c:1223-1251