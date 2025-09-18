# OidFunctionCall1Coll

## Location
src/backend/utils/fmgr/fmgr.c: 1411 - 1420

## Overview
A convenience function that calls a PostgreSQL function identified by its OID with one argument and a specific collation, handling the function manager setup automatically.

## Definition
```c
Datum OidFunctionCall1Coll(Oid functionId, Oid collation, Datum arg1)
```

## Detailed Description
OidFunctionCall1Coll is a wrapper function that simplifies calling PostgreSQL functions when you have the function's OID and need to pass exactly one argument with collation support. Like its sibling OidFunctionCall0Coll, it internally calls fmgr_info() to initialize the function manager information, then immediately invokes FunctionCall1Coll() to execute the function with the specified collation and single argument.

This function is commonly used in PostgreSQL's internal operations, particularly in hash and B-tree indexing operations where functions need to be called with collation-sensitive comparisons. The function is designed for occasional use; for repeated calls to the same function, it's more efficient to call fmgr_info() once and then use FunctionCall1Coll() directly.

## Parameters / Member Variables
- `functionId`: The OID (Object Identifier) of the function to be called
- `collation`: The collation OID to be used for collation-sensitive operations within the function
- `arg1`: The single Datum argument to pass to the function

## Dependencies
- Functions called/Symbols referenced:
  - fmgr_info (initializes function manager information)
  - FunctionCall1Coll (performs the actual function call with collation and one argument)
- Called from (representative examples):
  - _hash_datum2hashkey_type (hash indexing operations)
  - _bt_allequalimage (B-tree equality image operations)

## Notes and Other Information
- This function is part of the function manager (fmgr) subsystem in PostgreSQL
- Neither arguments nor result are allowed to be NULL according to the source comments
- The function automatically handles the FmgrInfo setup and cleanup
- Located in src/backend/utils/fmgr/fmgr.c at lines 1411-1420
- Widely used in indexing operations where single-argument function calls with collation are needed
- Part of a family including OidFunctionCall0Coll, OidFunctionCall2Coll, etc. for different argument counts