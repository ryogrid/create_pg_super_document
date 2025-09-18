# get_func_name

## Location
[src/backend/utils/cache/lsyscache.c:1608-1631](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1608-L1631)

## Overview
Returns a palloc'd copy of the function name for a given function OID, or NULL if the function doesn't exist.

## Definition
```c
char *get_func_name(Oid funcid)
```

## Detailed Description
This function retrieves the name of a PostgreSQL function from the system catalog given its OID. It performs a lookup in the pg_proc system catalog to find the function entry and extracts the function name from the proname field. The function returns a dynamically allocated copy of the function name string that must be freed by the caller using pfree().

The function is part of the function cache utilities in lsyscache.c and provides a convenient way to obtain human-readable function names for logging, error messages, and debugging purposes. It's commonly used throughout the PostgreSQL codebase when function names need to be displayed or logged.

## Parameters / Member Variables
- `funcid`: The OID of the function whose name should be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup for PROCOID)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract struct from tuple)
  - Form_pg_proc (procedure catalog structure)
  - NameStr (macro to extract string from Name type)
  - [pstrdup](../p/pstrdup.md) (palloc'd string duplication)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID to Datum conversion)

- Called from (representative examples):
  - [lookup_agg_function](../l/lookup_agg_function.md) (aggregate function processing)
  - [ExplainTargetRel](../E/ExplainTargetRel.md) (EXPLAIN output generation)
  - [AlterFunction](../A/AlterFunction.md) (function modification commands)
  - [ExecInitFunc](../E/ExecInitFunc.md) (executor function initialization)
  - [HandleFunctionRequest](../H/HandleFunctionRequest.md) (fastpath function calls)
  - print_expr (expression printing for debugging)

## Notes and Other Information
- This function is part of the lsyscache.c module which provides cached access to system catalog information
- Returns NULL if the function OID doesn't exist in pg_proc
- The returned string is allocated using palloc() and must be freed by the caller using pfree()
- This function is marked as part of the "FUNCTION CACHE" section in the source code
- Commonly used for error reporting, logging, and debugging where human-readable function names are needed
- The function accesses the proname field from the pg_proc catalog which stores the function's name as a Name type (fixed-length string)
- Used extensively throughout the executor, parser, and command processing modules
- Essential for generating meaningful error messages and diagnostic output that reference function names