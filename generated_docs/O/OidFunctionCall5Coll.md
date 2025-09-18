# OidFunctionCall5Coll

## Location
[src/backend/utils/fmgr/fmgr.c:1453-1463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1453-L1463)

## Overview
OidFunctionCall5Coll is a utility function that invokes a PostgreSQL function by its OID (Object Identifier) with five arguments and explicit collation support.

## Definition


## Detailed Description
This function provides a convenient wrapper for calling PostgreSQL functions identified by their OID when you need to pass exactly five arguments and specify a collation. It internally sets up the function manager information (FmgrInfo) for the given function OID and then delegates to FunctionCall5Coll to perform the actual function call. This is part of PostgreSQL's function manager (fmgr) system that handles dynamic function calls with type safety and collation awareness.

## Parameters / Member Variables
- : The OID of the function to be called
- : The OID of the collation to use for the function call
- : First argument to pass to the function (as Datum)
- : Second argument to pass to the function (as Datum)
- : Third argument to pass to the function (as Datum)
- : Fourth argument to pass to the function (as Datum)
- : Fifth argument to pass to the function (as Datum)

## Dependencies
- Functions called/Symbols referenced:
  - [fmgr_info](../f/fmgr_info.md)
  - [FunctionCall5Coll](../F/FunctionCall5Coll.md)
- Called from (representative examples):
  - [join_selectivity](../j/join_selectivity.md)
  - OidFunctionCall5 (macro)

## Notes and Other Information
- This function is part of a family of OidFunctionCallNColl functions that support different numbers of arguments (1-9)
- The function sets up a local FmgrInfo structure, which contains cached information about the function being called
- Collation support is important for text operations and sorting in PostgreSQL
- The function returns a Datum, which is PostgreSQL's generic data type for function return values
- Located in src/backend/utils/fmgr/fmgr.c at lines 1453-1463