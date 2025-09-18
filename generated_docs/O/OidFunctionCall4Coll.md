# OidFunctionCall4Coll

## Location
src/backend/utils/fmgr/fmgr.c: 1442 - 1452

## Overview
A convenience function that calls a PostgreSQL function identified by its OID with four arguments and a specific collation, handling the function manager setup automatically.

## Definition
```c
Datum OidFunctionCall4Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4)
```

## Detailed Description
OidFunctionCall4Coll is the highest-arity wrapper function in the OidFunctionCall*Coll family, designed for calling PostgreSQL functions that require exactly four arguments with collation support. Like its siblings, it internally calls fmgr_info() to initialize the function manager information, then immediately invokes FunctionCall4Coll() to execute the function with the specified collation and four arguments.

This function is used for complex operations that require four parameters, such as advanced selectivity estimation functions, complex string operations, or specialized analytical functions. It's particularly useful in query optimization scenarios where selectivity functions need multiple parameters to make accurate estimates.

## Parameters / Member Variables
- `functionId`: The OID (Object Identifier) of the function to be called
- `collation`: The collation OID to be used for collation-sensitive operations within the function
- `arg1`: The first Datum argument to pass to the function
- `arg2`: The second Datum argument to pass to the function
- `arg3`: The third Datum argument to pass to the function
- `arg4`: The fourth Datum argument to pass to the function

## Dependencies
- Functions called/Symbols referenced:
  - fmgr_info (initializes function manager information)
  - FunctionCall4Coll (performs the actual function call with collation and four arguments)
- Called from (representative examples):
  - restriction_selectivity (query optimization selectivity estimation)

## Notes and Other Information
- This function is part of the function manager (fmgr) subsystem in PostgreSQL
- Neither arguments nor result are allowed to be NULL according to the source comments
- The function automatically handles the FmgrInfo setup and cleanup
- Located in src/backend/utils/fmgr/fmgr.c at lines 1442-1452
- Represents the maximum arity in the OidFunctionCall*Coll family
- Primarily used in query optimization and complex analytical operations
- Part of a family including OidFunctionCall0Coll through OidFunctionCall3Coll for different argument counts