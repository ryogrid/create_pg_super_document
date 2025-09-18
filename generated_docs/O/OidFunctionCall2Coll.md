# OidFunctionCall2Coll

## Location
src/backend/utils/fmgr/fmgr.c: 1421 - 1430

## Overview
A convenience function that calls a PostgreSQL function identified by its OID with two arguments and a specific collation, handling the function manager setup automatically.

## Definition
```c
Datum OidFunctionCall2Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2)
```

## Detailed Description
OidFunctionCall2Coll is a wrapper function that simplifies calling PostgreSQL functions when you have the function's OID and need to pass exactly two arguments with collation support. Following the same pattern as its sibling functions, it internally calls fmgr_info() to initialize the function manager information, then immediately invokes FunctionCall2Coll() to execute the function with the specified collation and two arguments.

This function is particularly useful in comparison operations, constraint checking, and B-tree operations where binary functions need to be called with collation-sensitive behavior. It's commonly used in indexing and constraint validation scenarios where two values need to be compared or processed together.

## Parameters / Member Variables
- `functionId`: The OID (Object Identifier) of the function to be called
- `collation`: The collation OID to be used for collation-sensitive operations within the function
- `arg1`: The first Datum argument to pass to the function
- `arg2`: The second Datum argument to pass to the function

## Dependencies
- Functions called/Symbols referenced:
  - fmgr_info (initializes function manager information)
  - FunctionCall2Coll (performs the actual function call with collation and two arguments)
- Called from (representative examples):
  - _bt_compare_scankey_args (B-tree scan key comparison operations)
  - index_recheck_constraint (index constraint rechecking)

## Notes and Other Information
- This function is part of the function manager (fmgr) subsystem in PostgreSQL
- Neither arguments nor result are allowed to be NULL according to the source comments
- The function automatically handles the FmgrInfo setup and cleanup
- Located in src/backend/utils/fmgr/fmgr.c at lines 1421-1430
- Frequently used in comparison and constraint validation operations where binary functions with collation are needed
- Part of a family including OidFunctionCall0Coll, OidFunctionCall1Coll, etc. for different argument counts