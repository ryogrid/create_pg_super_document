# OidFunctionCall0Coll

## Location
[src/backend/utils/fmgr/fmgr.c:1401-1410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1401-L1410)

## Overview
A convenience function that calls a PostgreSQL function identified by its OID with no arguments and a specific collation, handling the function manager setup automatically.

## Definition

```c
Datum
OidFunctionCall0Coll(Oid functionId, Oid collation)
```
## Detailed Description
OidFunctionCall0Coll is a wrapper function that simplifies calling PostgreSQL functions when you have the function's OID but don't want to manually manage the FmgrInfo structure. It internally calls fmgr_info() to initialize the function manager information, then immediately invokes FunctionCall0Coll() to execute the function with the specified collation. This function is part of a family of OidFunctionCall*Coll functions that handle different numbers of arguments (0-4) while supporting collation-aware operations.

The function is designed for cases where a function needs to be called only once or infrequently. For repeated calls to the same function, it's more efficient to call fmgr_info() once and then use FunctionCall0Coll() directly to avoid the overhead of repeated function lookup.

## Parameters / Member Variables
- : The OID (Object Identifier) of the function to be called
- : The collation OID to be used for collation-sensitive operations within the function

## Dependencies
- Functions called/Symbols referenced:
  - [fmgr_info](../f/fmgr_info.md) (initializes function manager information)
  - [FunctionCall0Coll](../F/FunctionCall0Coll.md) (performs the actual function call with collation)
- Called from (representative examples):
  - Various PostgreSQL internal functions that need to invoke other functions by OID with collation support

## Notes and Other Information
- This function is part of the function manager (fmgr) subsystem in PostgreSQL
- Neither arguments nor result are allowed to be NULL according to the source comments
- The function automatically handles the FmgrInfo setup and cleanup
- Located in src/backend/utils/fmgr/fmgr.c at lines 1401-1410
- Part of a family including OidFunctionCall1Coll, OidFunctionCall2Coll, etc. for different argument counts