# FunctionCall1Coll

## Location
[src/backend/utils/fmgr/fmgr.c:1129-1148](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1129-L1148)

## Overview
FunctionCall1Coll is a utility function that invokes a previously-looked-up PostgreSQL function with one parameter and an explicit collation setting.

## Definition

```c
Datum
FunctionCall1Coll(FmgrInfo *flinfo, Oid collation, Datum arg1)
```
## Detailed Description
This function is part of PostgreSQL's function manager (fmgr) system that provides a high-level interface for calling database functions. FunctionCall1Coll handles the case where a function needs to be called with exactly one argument and a specific collation context. The function sets up the necessary function call information structure, populates the single argument, invokes the target function, and performs error checking to ensure the result is not NULL.

The function creates a local FunctionCallInfoData structure with space for 1 argument, initializes it with the provided function info and collation, sets the argument value and null indicator, then calls the actual function through FunctionCallInvoke.

## Parameters / Member Variables
- : Pointer to FmgrInfo structure containing the previously-looked-up function information
- : OID of the collation to be used during function execution
- : The single Datum argument to pass to the function

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for creating local FunctionCallInfoData)
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - elog (for error reporting)
- Called from (representative examples):
  - [brin_bloom_add_value](../b/brin_bloom_add_value.md)
  - [brin_bloom_consistent](../b/brin_bloom_consistent.md)  
  - [brin_inclusion_add_value](../b/brin_inclusion_add_value.md)
  - [gistdentryinit](../g/gistdentryinit.md)
  - [_hash_datum2hashkey](../h/_hash_datum2hashkey.md)
  - [ExecHashGetHashValue](../E/ExecHashGetHashValue.md)
  - [hash_multirange](../h/hash_multirange.md)
  - [hash_range](../h/hash_range.md)
  - [OidFunctionCall1Coll](../O/OidFunctionCall1Coll.md)

## Notes and Other Information
- This function explicitly checks for NULL results and throws an ERROR if the called function returns NULL
- Part of a family of FunctionCallNColl functions (0-4 parameters) that provide collation-aware function calling interfaces
- The collation parameter allows for locale-sensitive operations in functions that support collation
- Widely used throughout PostgreSQL for hash functions, BRIN index operations, GiST operations, and various data type operations
- Located in src/backend/utils/fmgr/fmgr.c:1129-1148

## Simplified Source

```c
Datum FunctionCall1Coll(FmgrInfo *flinfo, Oid collation, Datum arg1) {
    LOCAL_FCINFO(fcinfo, 1);  // Create local function call info for 1 arg
    Datum result;

    // Initialize function call information with collation
    InitFunctionCallInfoData(*fcinfo, flinfo, 1, collation, NULL, NULL);

    // Set the single argument
    fcinfo->args[0].value = arg1;
    fcinfo->args[0].isnull = false;

    // Invoke the function
    result = FunctionCallInvoke(fcinfo);

    // Check for unexpected NULL result
    if (fcinfo->isnull)
        elog(ERROR, "function %u returned NULL", flinfo->fn_oid);

    return result;
}
```