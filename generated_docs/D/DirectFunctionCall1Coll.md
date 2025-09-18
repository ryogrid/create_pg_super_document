# DirectFunctionCall1Coll

## Location
src/backend/utils/fmgr/fmgr.c: 792 - 811

## Overview
Directly invokes a PostgreSQL function with one argument and a specified collation, bypassing the standard function manager lookup mechanisms.

## Definition


## Detailed Description
The  function provides a high-performance mechanism for calling PostgreSQL functions when the function pointer is already known and only one argument needs to be passed. This function is part of PostgreSQL's direct function call family, designed to minimize overhead when the function identity is determined at compile time or through previous lookups.

Unlike the standard fmgr function calling mechanism, this function bypasses catalog lookups and FmgrInfo structures, making it significantly faster for cases where the function pointer is directly available. The function sets up a minimal FunctionCallInfo structure, populates it with the single argument and collation information, then directly invokes the target function.

The function enforces strict non-NULL semantics - neither the argument nor the result can be NULL, and it will raise an error if the called function returns NULL. This makes it suitable for use in performance-critical code paths where NULL handling is not required.

## Parameters / Member Variables
- : PGFunction pointer to the actual function to be called
- : Oid specifying the collation to be used for the function call
- : Datum containing the single argument value to pass to the function

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for stack-allocated FunctionCallInfo)
  - InitFunctionCallInfoData
  - elog (for error reporting)
- Called from (representative examples):
  - [Generic_Text_IC_like](../G/Generic_Text_IC_like.md)
  - texthashfast
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