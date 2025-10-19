# get_func_nargs

## Location
[src/backend/utils/cache/lsyscache.c:1674-1695](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1674-L1695)

## Overview
Returns the number of arguments (parameters) that a given function accepts by looking up the function in the system catalog.

## Definition

```c
struct;
```
## Detailed Description
This function retrieves the argument count for a specified function by performing a system cache lookup on the pg_proc table. It extracts the pronargs field from the function's catalog entry, which contains the number of input arguments the function expects. Like get_func_rettype, this function throws an error if the function is not found rather than returning a default value.

## Parameters / Member Variables
- : The OID of the function whose argument count is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md): Searches the system cache for the function entry
  - HeapTupleIsValid: Validates the returned heap tuple
  - elog: Logs error if function not found
  - GETSTRUCT: Extracts the struct from the heap tuple
  - [ReleaseSysCache](../R/ReleaseSysCache.md): Releases the system cache entry
  - Form_pg_proc: PostgreSQL system catalog structure for procedures/functions
- Called from (representative examples):
  - Currently no direct references found in the indexed codebase

## Notes and Other Information
- Part of PostgreSQL's system catalog lookup utilities in lsyscache.c
- Throws ERROR if function does not exist, ensuring strict validation
- Returns the total number of input arguments including default parameters
- Does not distinguish between required and optional parameters in the count
- Useful for function signature validation and dynamic function calling
- May be used internally by other PostgreSQL components not captured in current indexing

## Simplified Source

```c
int get_func_nargs(Oid funcid) {
    // Look up function in system cache
    HeapTuple tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));

    // Ensure function exists
    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for function %u", funcid);

    // Extract argument count from pg_proc entry
    int result = ((Form_pg_proc) GETSTRUCT(tp))->pronargs;

    // Clean up cache reference
    ReleaseSysCache(tp);
    return result;
}
```