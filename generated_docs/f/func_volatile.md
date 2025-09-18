# func_volatile

## Location
[src/backend/utils/cache/lsyscache.c:1780-1798](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1780-L1798)

## Overview
Retrieves the provolatile flag for a given function, indicating the function's volatility category which affects optimization and caching behavior.

## Definition
```c
char func_volatile(Oid funcid)
```

## Detailed Description
This function performs a system cache lookup to retrieve the provolatile attribute of a PostgreSQL function. The provolatile flag is a character value stored in the pg_proc system catalog that indicates the function's volatility category:

- PROVOLATILE_IMMUTABLE ('i'): Function cannot modify the database and always returns the same result for the same arguments
- PROVOLATILE_STABLE ('s'): Function cannot modify the database but may return different results within a single table scan
- PROVOLATILE_VOLATILE ('v'): Function may modify the database or return different results on successive calls with the same arguments

This information is critical for query optimization, determining when functions can be pre-evaluated, cached, or moved around in the query plan.

## Parameters / Member Variables
- `funcid`: Object identifier (Oid) of the function whose provolatile flag is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
- Called from (representative examples):
  - [contain_mutable_or_user_functions_checker](../c/contain_mutable_or_user_functions_checker.md) (src/backend/commands/publicationcmds.c:440)
  - [findTypeInputFunction](findTypeInputFunction.md) (src/backend/commands/typecmds.c:2006)
  - [contain_mutable_functions_checker](../c/contain_mutable_functions_checker.md) (src/backend/optimizer/util/clauses.c:378)
  - [contain_volatile_functions_checker](../c/contain_volatile_functions_checker.md) (src/backend/optimizer/util/clauses.c:546)
  - [ece_function_is_safe](../e/ece_function_is_safe.md) (src/backend/optimizer/util/clauses.c:3754)

## Notes and Other Information
- Part of the lsyscache.c module which provides convenient access functions for system catalog information
- Essential for query optimization decisions such as constant folding, predicate pushdown, and expression evaluation timing
- Used extensively in the optimizer to determine when expressions can be pre-computed or cached
- The volatility category affects whether functions can be executed at plan time vs. execution time
- IMMUTABLE functions enable the most aggressive optimizations, while VOLATILE functions require careful handling
- Located in src/backend/utils/cache/lsyscache.c:1780-1798