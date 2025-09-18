# FunctionCall3Coll

## Location
src/backend/utils/fmgr/fmgr.c: 1171 - 1195

## Overview
FunctionCall3Coll is a utility function that invokes a previously-looked-up PostgreSQL function with three parameters and an explicit collation setting.

## Definition


## Detailed Description
This function is part of PostgreSQL's function manager (fmgr) system that provides a high-level interface for calling database functions. FunctionCall3Coll handles the case where a function needs to be called with exactly three arguments and a specific collation context. The function sets up the necessary function call information structure, populates all three arguments, invokes the target function, and performs error checking to ensure the result is not NULL.

The function creates a local FunctionCallInfoData structure with space for 3 arguments, initializes it with the provided function info and collation, sets all three argument values and their null indicators to false, then calls the actual function through FunctionCallInvoke.

## Parameters / Member Variables
- : Pointer to FmgrInfo structure containing the previously-looked-up function information
- : OID of the collation to be used during function execution
- : The first Datum argument to pass to the function
- : The second Datum argument to pass to the function
- : The third Datum argument to pass to the function

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for creating local FunctionCallInfoData)
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - elog (for error reporting)
- Called from (representative examples):
  - bringetbitmap
  - union_tuples
  - ginExtractEntries
  - gistKeyIsEQ
  - gistpenalty
  - OidFunctionCall3Coll

## Notes and Other Information
- This function explicitly checks for NULL results and throws an ERROR if the called function returns NULL
- Part of a family of FunctionCallNColl functions (0-4 parameters) that provide collation-aware function calling interfaces
- The collation parameter allows for locale-sensitive operations in functions that support collation
- Used primarily for specialized index operations in BRIN, GIN, and GiST access methods
- Less commonly used compared to FunctionCall1Coll and FunctionCall2Coll, but essential for complex index operations requiring three parameters
- Located in src/backend/utils/fmgr/fmgr.c:1171-1195