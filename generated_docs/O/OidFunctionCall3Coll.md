# OidFunctionCall3Coll

## Location
[src/backend/utils/fmgr/fmgr.c:1431-1441](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1431-L1441)

## Overview
A convenience function that calls a PostgreSQL function identified by its OID with three arguments and a specific collation, handling the function manager setup automatically.

## Definition
```c
Datum OidFunctionCall3Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3)
```

## Detailed Description
OidFunctionCall3Coll is a wrapper function that simplifies calling PostgreSQL functions when you have the function's OID and need to pass exactly three arguments with collation support. Following the established pattern of the OidFunctionCall*Coll family, it internally calls fmgr_info() to initialize the function manager information, then immediately invokes FunctionCall3Coll() to execute the function with the specified collation and three arguments.

This function is used for more complex operations that require three parameters, such as substring operations, range functions, or specialized comparison functions that need additional context. While less commonly used than the 0-2 argument variants, it provides essential functionality for operations requiring ternary functions with collation awareness.

## Parameters / Member Variables
- `functionId`: The OID (Object Identifier) of the function to be called
- `collation`: The collation OID to be used for collation-sensitive operations within the function
- `arg1`: The first Datum argument to pass to the function
- `arg2`: The second Datum argument to pass to the function
- `arg3`: The third Datum argument to pass to the function

## Dependencies
- Functions called/Symbols referenced:
  - [fmgr_info](../f/fmgr_info.md) (initializes function manager information)
  - [FunctionCall3Coll](../F/FunctionCall3Coll.md) (performs the actual function call with collation and three arguments)
- Called from (representative examples):
  - Various PostgreSQL internal functions that need to invoke ternary functions with collation support

## Notes and Other Information
- This function is part of the function manager (fmgr) subsystem in PostgreSQL
- Neither arguments nor result are allowed to be NULL according to the source comments
- The function automatically handles the FmgrInfo setup and cleanup
- Located in src/backend/utils/fmgr/fmgr.c at lines 1431-1441
- Less frequently used than lower-arity variants but essential for complex operations requiring three arguments
- Part of a family including OidFunctionCall0Coll, OidFunctionCall1Coll, etc. for different argument counts

## Simplified Source

```c
Datum OidFunctionCall3Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2,
                           Datum arg3) {
    // Set up function manager info for the given function OID
    FmgrInfo flinfo;
    fmgr_info(functionId, &flinfo);

    // Call the function with collation support and three arguments
    return FunctionCall3Coll(&flinfo, collation, arg1, arg2, arg3);
}
```