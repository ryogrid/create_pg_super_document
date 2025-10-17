# OidFunctionCall6Coll

## Location
[src/backend/utils/fmgr/fmgr.c:1464-1476](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1464-L1476)

## Overview
OidFunctionCall6Coll is a utility function that invokes a PostgreSQL function by its OID (Object Identifier) with six arguments and explicit collation support.

## Definition
```c
Datum OidFunctionCall6Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5, Datum arg6)
```

## Detailed Description
This function provides a convenient wrapper for calling PostgreSQL functions identified by their OID when you need to pass exactly six arguments and specify a collation. It internally sets up the function manager information (FmgrInfo) for the given function OID and then delegates to FunctionCall6Coll to perform the actual function call. This is part of PostgreSQL's function manager (fmgr) system that handles dynamic function calls with type safety and collation awareness.

## Parameters / Member Variables
- `functionId`: The OID of the function to be called
- `collation`: The OID of the collation to use for the function call
- `arg1`: First argument to pass to the function (as Datum)
- `arg2`: Second argument to pass to the function (as Datum)
- `arg3`: Third argument to pass to the function (as Datum)
- `arg4`: Fourth argument to pass to the function (as Datum)
- `arg5`: Fifth argument to pass to the function (as Datum)
- `arg6`: Sixth argument to pass to the function (as Datum)

## Dependencies
- Functions called/Symbols referenced:
  - [fmgr_info](../f/fmgr_info.md)
  - [FunctionCall6Coll](../F/FunctionCall6Coll.md)
- Called from (representative examples):
  - OidFunctionCall6 (macro)

## Notes and Other Information
- This function is part of a family of OidFunctionCallNColl functions that support different numbers of arguments (1-9)
- The function sets up a local FmgrInfo structure, which contains cached information about the function being called
- Collation support is important for text operations and sorting in PostgreSQL
- The function returns a Datum, which is PostgreSQL's generic data type for function return values
- Located in src/backend/utils/fmgr/fmgr.c at lines 1464-1476

## Simplified Source

```c
Datum OidFunctionCall6Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2,
                           Datum arg3, Datum arg4, Datum arg5, Datum arg6) {
    // Set up function manager info for the given function OID
    FmgrInfo flinfo;
    fmgr_info(functionId, &flinfo);

    // Call the function with collation support and six arguments
    return FunctionCall6Coll(&flinfo, collation, arg1, arg2, arg3, arg4, arg5, arg6);
}
```