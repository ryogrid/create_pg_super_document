# FunctionCall4Coll

## Location
[src/backend/utils/fmgr/fmgr.c:1196-1222](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1196-L1222)

## Overview
FunctionCall4Coll is a utility function that invokes a previously-looked-up PostgreSQL function with four parameters and an explicit collation setting.

## Definition


## Detailed Description
This function is part of PostgreSQL's function manager (fmgr) system that provides a high-level interface for calling database functions. FunctionCall4Coll handles the case where a function needs to be called with exactly four arguments and a specific collation context. The function sets up the necessary function call information structure, populates all four arguments, invokes the target function, and performs error checking to ensure the result is not NULL.

The function creates a local FunctionCallInfoData structure with space for 4 arguments, initializes it with the provided function info and collation, sets all four argument values and their null indicators to false, then calls the actual function through FunctionCallInvoke.

## Parameters / Member Variables
- : Pointer to FmgrInfo structure containing the previously-looked-up function information
- : OID of the collation to be used during function execution
- : The first Datum argument to pass to the function
- : The second Datum argument to pass to the function
- : The third Datum argument to pass to the function
- : The fourth Datum argument to pass to the function

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for creating local FunctionCallInfoData)
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - elog (for error reporting)
- Called from (representative examples):
  - [bringetbitmap](../b/bringetbitmap.md)
  - [add_values_to_range](../a/add_values_to_range.md)
  - [collectMatchBitmap](../c/collectMatchBitmap.md)
  - [matchPartialInPendingList](../m/matchPartialInPendingList.md)
  - scalararraysel
  - [OidFunctionCall4Coll](../O/OidFunctionCall4Coll.md)

## Notes and Other Information
- This function explicitly checks for NULL results and throws an ERROR if the called function returns NULL
- Part of a family of FunctionCallNColl functions (0-4 parameters) that provide collation-aware function calling interfaces
- The collation parameter allows for locale-sensitive operations in functions that support collation
- The least commonly used among the FunctionCallNColl family, reserved for complex operations requiring four parameters
- Used primarily for specialized index operations in BRIN and GIN access methods, as well as selectivity estimation functions
- Represents the maximum number of direct parameters supported by the FunctionCallNColl family
- Located in src/backend/utils/fmgr/fmgr.c:1196-1222