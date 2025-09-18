# get_func_rettype

## Location
src/backend/utils/cache/lsyscache.c: 1655 - 1673

## Overview
Returns the result type OID of a given function by looking up the function's return type in the system catalog.

## Definition


## Detailed Description
This function retrieves the return type OID for a specified function by performing a system cache lookup on the pg_proc table. It extracts the prorettype field from the function's catalog entry, which contains the OID of the function's return type. Unlike get_func_namespace, this function throws an error if the function is not found rather than returning InvalidOid, making it more strict about function existence.

## Parameters / Member Variables
- : The OID of the function whose return type is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1: Searches the system cache for the function entry
  - HeapTupleIsValid: Validates the returned heap tuple
  - elog: Logs error if function not found
  - GETSTRUCT: Extracts the struct from the heap tuple
  - ReleaseSysCache: Releases the system cache entry
  - Form_pg_proc: PostgreSQL system catalog structure for procedures/functions
- Called from (representative examples):
  - OperatorCreate: Used in operator creation to validate function return types
  - CreateConversionCommand: Used in conversion command creation
  - CreateEventTrigger: Used in event trigger creation
  - Various type validation functions in typecmds.c
  - Access method and foreign data wrapper handlers

## Notes and Other Information
- Part of PostgreSQL's system catalog lookup utilities in lsyscache.c
- Throws ERROR if function does not exist, making it stricter than some other lookup functions
- Heavily used throughout the system for type checking and validation
- The return type OID can be used to look up detailed type information via other catalog functions
- Critical for function signature validation and type system enforcement