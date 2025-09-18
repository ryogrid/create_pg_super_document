# get_func_signature

## Location
[src/backend/utils/cache/lsyscache.c:1696-1722](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1696-L1722)

## Overview
Returns the complete function signature including argument types array and return type for a given function OID.

## Definition


## Detailed Description
This function retrieves the complete signature information for a specified function by performing a system cache lookup on the pg_proc table. It extracts both the argument types array and the return type from the function's catalog entry. The function allocates memory for the argument types array and copies the type OIDs from the catalog. It returns the return type OID and sets the output parameters for argument types and count.

## Parameters / Member Variables
- : The OID of the function whose signature is to be retrieved
- : Output parameter - pointer to receive the allocated array of argument type OIDs
- : Output parameter - pointer to receive the number of arguments

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md): Searches the system cache for the function entry
  - HeapTupleIsValid: Validates the returned heap tuple
  - elog: Logs error if function not found
  - GETSTRUCT: Extracts the struct from the heap tuple
  - Assert: Validates consistency between pronargs and proargtypes.dim1
  - [palloc](../p/palloc.md): Allocates memory for the argument types array
  - memcpy: Copies argument types from catalog to allocated array
  - [ReleaseSysCache](../R/ReleaseSysCache.md): Releases the system cache entry
  - Form_pg_proc: PostgreSQL system catalog structure for procedures/functions
- Called from (representative examples):
  - [typeDepNeeded](../t/typeDepNeeded.md): Used in operator class dependency checking
  - [resolve_aggregate_transtype](../r/resolve_aggregate_transtype.md): Used in aggregate function type resolution
  - [plperl_call_perl_func](../p/plperl_call_perl_func.md): Used in PL/Perl function calling

## Notes and Other Information
- Part of PostgreSQL's system catalog lookup utilities in lsyscache.c
- Allocates memory using palloc - caller is responsible for freeing the argtypes array
- Throws ERROR if function does not exist, ensuring strict validation
- Validates consistency between argument count and array dimensions using Assert
- Provides complete signature information needed for function signature matching
- Critical for dynamic function calling and type checking in language handlers
- Used extensively in procedural language implementations and aggregate functions