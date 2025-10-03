# DirectFunctionCall1Coll

## Location
[src/backend/utils/fmgr/fmgr.c:792-811](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L792-L811)

## Overview
Directly invokes a PostgreSQL function with one argument and a specified collation, bypassing the standard function manager lookup mechanisms.

## Definition

```c
Datum
DirectFunctionCall1Coll(PGFunction func, Oid collation, Datum arg1)
```
## Detailed Description
The  function provides a high-performance mechanism for calling PostgreSQL functions when the function pointer is already known and only one argument needs to be passed. This function is part of PostgreSQL's direct function call family, designed to minimize overhead when the function identity is determined at compile time or through previous lookups.

Unlike the standard fmgr function calling mechanism, this function bypasses catalog lookups and FmgrInfo structures, making it significantly faster for cases where the function pointer is directly available. The function sets up a minimal FunctionCallInfo structure, populates it with the single argument and collation information, then directly invokes the target function.

The function enforces strict non-NULL semantics - neither the argument nor the result can be NULL, and it will raise an error if the called function returns NULL. This makes it suitable for use in performance-critical code paths where NULL handling is not required.

## Parameters / Member Variables
- `func`: PGFunction pointer to the actual function to be called
- `collation`: Oid specifying the collation to be used for the function call
- `arg1`: Datum containing the single argument value to pass to the function
## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for stack-allocated FunctionCallInfo)
  - InitFunctionCallInfoData
  - elog (for error reporting)
- Called from (representative examples):
  - [Generic_Text_IC_like](../G/Generic_Text_IC_like.md)
  - [texthashfast](../t/texthashfast.md)
  - [libpqrcv_create_slot](../l/libpqrcv_create_slot.md)
  - DirectFunctionCall1 (macro wrapper)

## Notes and Other Information
- Part of the DirectFunctionCall family providing direct function invocation
- Uses stack-allocated FunctionCallInfo for performance
- Enforces non-NULL argument and result semantics
- Cannot be used with functions that need to examine FmgrInfo structures
- Collation-aware version of DirectFunctionCall1
- Significantly faster than standard fmgr calling conventions
- Commonly used in performance-critical internal PostgreSQL code
- The function pointer must be obtained through other means (e.g., fmgr_symbol lookup)

## Simplified Source

```c
Datum DirectFunctionCall1Coll(PGFunction func, Oid collation, Datum arg1) {
    LOCAL_FCINFO(fcinfo, 1);
    Datum result;

    // Initialize function call info with 1 argument and specified collation
    InitFunctionCallInfoData(*fcinfo, NULL, 1, collation, NULL, NULL);

    // Set the single argument as non-null
    fcinfo->args[0].value = arg1;
    fcinfo->args[0].isnull = false;

    // Call the function directly
    result = (*func)(fcinfo);

    // Ensure function didn't return NULL unexpectedly
    if (fcinfo->isnull)
        elog(ERROR, "function %p returned NULL", (void *) func);

    return result;
}
```