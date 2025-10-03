# get_func_retset

## Location
[src/backend/utils/cache/lsyscache.c:1742-1760](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1742-L1760)

## Overview
Retrieves the proretset flag for a given function, indicating whether the function returns a set of values rather than a single value.

## Definition

```c
bool
get_func_retset(Oid funcid)
```
## Detailed Description
This function performs a system cache lookup to retrieve the proretset attribute of a PostgreSQL function. The proretset flag is a boolean value stored in the pg_proc system catalog that indicates whether a function returns a set of rows (true) or a single row/value (false). This information is crucial for query planning and execution, as set-returning functions require different handling than scalar functions.

The function uses PostgreSQL's system cache mechanism (SearchSysCache1) for efficient lookups, avoiding direct table scans of pg_proc. If the function ID is not found in the cache, an ERROR is raised.

## Parameters / Member Variables
- `funcid`: Object identifier (Oid) of the function whose proretset flag is to be retrieved
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
- Called from (representative examples):
  - [make_op](../m/make_op.md) (src/backend/parser/parse_oper.c:747)
  - [make_scalar_array_op](../m/make_scalar_array_op.md) (src/backend/parser/parse_oper.c:846)

## Notes and Other Information
- Part of the lsyscache.c module which provides convenient access functions for system catalog information
- Uses the PROCOID cache for efficient function metadata lookups
- Raises an ERROR if the function ID is invalid or not found
- The proretset flag is essential for distinguishing scalar functions from set-returning functions (SRFs)
- Located in src/backend/utils/cache/lsyscache.c:1742-1760

## Simplified Source

```c
bool get_func_retset(Oid funcid) {
    // Look up function in system cache
    HeapTuple tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));

    // Error if function not found
    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for function %u", funcid);

    // Extract the proretset flag from pg_proc tuple
    bool result = ((Form_pg_proc) GETSTRUCT(tp))->proretset;

    // Clean up and return
    ReleaseSysCache(tp);
    return result;
}
```