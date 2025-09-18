# FunctionCall0Coll

## Location
src/backend/utils/fmgr/fmgr.c: 1112 - 1128

## Overview
FunctionCall0Coll is a utility function that invokes a previously-looked-up PostgreSQL function with no parameters and an explicit collation setting.

## Definition


## Detailed Description
This function is part of PostgreSQL's function manager (fmgr) system that provides a high-level interface for calling database functions. FunctionCall0Coll specifically handles the case where a function needs to be called with zero arguments but with a specific collation context. The function sets up the necessary function call information structure, invokes the target function, and performs error checking to ensure the result is not NULL (since the caller is clearly not expecting a NULL result).

The function uses the LOCAL_FCINFO macro to create a local FunctionCallInfoData structure with space for 0 arguments, initializes it with the provided function info and collation, then calls the actual function through FunctionCallInvoke.

## Parameters / Member Variables
- : Pointer to FmgrInfo structure containing the previously-looked-up function information
- : OID of the collation to be used during function execution

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for creating local FunctionCallInfoData)
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - elog (for error reporting)
- Called from (representative examples):
  - OidFunctionCall0Coll

## Notes and Other Information
- This function explicitly checks for NULL results and throws an ERROR if the called function returns NULL, indicating that callers expect a non-NULL return value
- Part of a family of FunctionCallNColl functions (0-4 parameters) that provide collation-aware function calling interfaces
- The collation parameter allows for locale-sensitive operations in functions that support collation
- Located in src/backend/utils/fmgr/fmgr.c:1112-1128