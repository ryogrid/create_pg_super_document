# get_typlenbyval

## Location
[src/backend/utils/cache/lsyscache.c:2251-2270](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2251-L2270)

## Overview
Efficiently retrieves both the storage length and pass-by-value status of a PostgreSQL data type in a single system cache lookup, providing essential type information needed for proper Datum handling and memory management.

## Definition
```c
void get_typlenbyval(Oid typid, int16 *typlen, bool *typbyval)
```

## Detailed Description
The `get_typlenbyval` function is an optimized utility that combines the functionality of `get_typlen` and `get_typbyval` into a single function call. Since both the type length (`typlen`) and pass-by-value status (`typbyval`) are frequently needed together for proper Datum handling, this function performs a single system cache lookup to retrieve both values simultaneously, improving performance over separate function calls.

This function is particularly important for code that needs to copy, serialize, or manipulate Datum values, as both pieces of information are essential for determining how to handle the data correctly. Unlike the individual functions, this function raises an ERROR if the type OID is invalid, making it more suitable for cases where type validity is expected.

The function is widely used throughout PostgreSQL's executor, optimizer, and utility code where type information is critical for correct data handling.

## Parameters / Member Variables
- `typid`: The OID (Object Identifier) of the PostgreSQL data type to look up
- `typlen`: Pointer to int16 where the type's storage length will be stored (-1 for variable-length types)
- `typbyval`: Pointer to bool where the type's pass-by-value status will be stored (true if passed by value, false if by reference)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract struct from heap tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - elog (error logging and reporting)
  - Form_pg_type (type catalog structure)
- Called from (representative examples):
  - [EstimateParamExecSpace](../E/EstimateParamExecSpace.md) (parallel execution parameter estimation)
  - [SerializeParamExecParams](../S/SerializeParamExecParams.md) (parameter serialization)
  - [init_sql_fcache](../i/init_sql_fcache.md) (SQL function cache initialization)
  - [ExecInitAgg](../E/ExecInitAgg.md) (aggregate node initialization)
  - [build_pertrans_for_aggref](../b/build_pertrans_for_aggref.md) (aggregate transition setup)
  - [ExecInitIndexScan](../E/ExecInitIndexScan.md) (index scan initialization)
  - [ExecWindowAgg](../E/ExecWindowAgg.md) (window function execution)
  - [makeNullConst](../m/makeNullConst.md) (null constant creation)
  - tupleso rt_begin_datum (datum tuple sorting)

## Notes and Other Information
- Raises an ERROR (not a return code) if the type OID is invalid or not found
- More efficient than calling `get_typlen` and `get_typbyval` separately
- Essential for any code that needs to copy, compare, or serialize Datum values
- The combination of typlen and typbyval determines the complete strategy for handling a type's values
- Used extensively in executor nodes, optimization, and serialization contexts
- Prefer this function over separate calls when both values are needed