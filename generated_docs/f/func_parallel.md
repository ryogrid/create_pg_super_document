# func_parallel

## Location
[src/backend/utils/cache/lsyscache.c:1799-1817](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1799-L1817)

## Overview
Retrieves the proparallel flag for a given function, indicating the function's parallel safety level for use in parallel query execution.

## Definition
```c
char func_parallel(Oid funcid)
```

## Detailed Description
This function performs a system cache lookup to retrieve the proparallel attribute of a PostgreSQL function. The proparallel flag is a character value stored in the pg_proc system catalog that indicates the function's parallel safety category:

- PROPARALLEL_SAFE ('s'): Function is safe to run in parallel mode without restrictions
- PROPARALLEL_RESTRICTED ('r'): Function can be run in parallel mode but only in the parallel leader process
- PROPARALLEL_UNSAFE ('u'): Function cannot be run in parallel mode at all

This information is essential for PostgreSQL's parallel query execution feature, determining whether and how functions can be executed when parallel workers are involved.

## Parameters / Member Variables
- `funcid`: Object identifier (Oid) of the function whose proparallel flag is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
- Called from (representative examples):
  - [set_rel_consider_parallel](../s/set_rel_consider_parallel.md) (src/backend/optimizer/path/allpaths.c:628)
  - [max_parallel_hazard_checker](../m/max_parallel_hazard_checker.md) (src/backend/optimizer/util/clauses.c:824)

## Notes and Other Information
- Part of the lsyscache.c module which provides convenient access functions for system catalog information
- Critical for parallel query planning and execution in PostgreSQL
- Used by the optimizer to determine parallel safety of query plans involving function calls
- UNSAFE functions prevent parallel execution entirely, while RESTRICTED functions limit parallelism
- The parallel safety level affects whether parallel workers can be used for query execution
- Introduced as part of PostgreSQL's parallel query execution infrastructure
- Located in src/backend/utils/cache/lsyscache.c:1799-1817