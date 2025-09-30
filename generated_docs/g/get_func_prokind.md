# get_func_prokind

## Location
[src/backend/utils/cache/lsyscache.c:1818-1836](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1818-L1836)

## Overview
Retrieves the prokind flag for a given routine, indicating the type of routine (function, procedure, aggregate, or window function).

## Definition
```c
char get_func_prokind(Oid funcid)
```

## Detailed Description
This function performs a system cache lookup to retrieve the prokind attribute of a PostgreSQL routine. The prokind flag is a character value stored in the pg_proc system catalog that indicates the kind of routine:

- PROKIND_FUNCTION ('f'): Regular function
- PROKIND_AGGREGATE ('a'): Aggregate function  
- PROKIND_WINDOW ('w'): Window function
- PROKIND_PROCEDURE ('p'): Stored procedure

This information is essential for distinguishing between different types of callable routines in PostgreSQL, as they have different calling conventions, syntax requirements, and behavioral characteristics.

## Parameters / Member Variables
- `funcid`: Object identifier (Oid) of the routine whose prokind flag is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
- Called from (representative examples):
  - [RemoveObjects](../R/RemoveObjects.md) (src/backend/commands/dropcmds.c:93)
  - [LookupFuncNameInternal](../L/LookupFuncNameInternal.md) (src/backend/parser/parse_func.c:2092, 2097)
  - [LookupFuncWithArgs](../L/LookupFuncWithArgs.md) (src/backend/parser/parse_func.c:2355, 2365, 2375)

## Notes and Other Information
- Part of the lsyscache.c module which provides convenient access functions for system catalog information
- Critical for parsing and semantic analysis to distinguish between functions, procedures, aggregates, and window functions
- Used extensively in function lookup and resolution during SQL parsing
- The prokind distinction affects syntax validation, calling conventions, and execution behavior
- Procedures (PROKIND_PROCEDURE) can be called with CALL statements but not in expressions
- Functions can be called in expressions, while procedures require different handling
- Located in src/backend/utils/cache/lsyscache.c:1818-1836

## Simplified Source

```c
char
get_func_prokind(Oid funcid)
{
    HeapTuple tp;
    char result;

    // Look up function in system cache
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for function %u", funcid);

    // Extract prokind field from pg_proc tuple
    result = ((Form_pg_proc) GETSTRUCT(tp))->prokind;
    ReleaseSysCache(tp);
    return result;
}
```