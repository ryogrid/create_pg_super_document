# get_func_result_name

## Location
[src/backend/utils/fmgr/funcapi.c:1607-1704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L1607-L1704)

## Overview
Retrieves the name of a function's single named output parameter, used to determine default column names for scalar function results.

## Definition

```c
struct_array()
		 * since the array data is just going to look like a C array of
		 * values.
		 */
		arr = DatumGetArrayTypeP(proargmodes);
```
## Detailed Description
This function examines a PostgreSQL function's metadata to determine if it has exactly one output parameter that is named. If such a parameter exists, it returns the parameter's name as a palloc'd string, which can be used as the default output column name for functions returning scalar types. If the function has no output parameters, multiple output parameters, or the single output parameter is unnamed, the function returns NULL.

The function accesses the pg_proc system catalog to retrieve the function's argument modes (proargmodes) and argument names (proargnames) arrays. It then scans through these arrays to identify output parameters (OUT, INOUT, or TABLE mode) and checks if exactly one named output parameter exists.

## Parameters / Member Variables
- : The OID of the function to examine in the pg_proc system catalog

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - [heap_attisnull](../h/heap_attisnull.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - DatumGetArrayTypeP
  - ARR_DIMS, ARR_NDIM, ARR_HASNULL, ARR_ELEMTYPE, ARR_DATA_PTR
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md)
  - TextDatumGetCString
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - PROARGMODE_IN, PROARGMODE_VARIADIC, PROARGMODE_OUT, PROARGMODE_INOUT, PROARGMODE_TABLE
- Called from (representative examples):
  - [chooseScalarFunctionAlias](../c/chooseScalarFunctionAlias.md)
  - TypeFuncClass

## Notes and Other Information
- Returns a palloc'd string that the caller is responsible for freeing
- Only considers functions with exactly one output parameter; multiple output parameters result in NULL return
- Checks both that the parameter exists and that it has a non-empty name
- Used primarily in query planning to determine appropriate column aliases for function results
- The function validates array structure and throws errors for malformed proargmodes or proargnames arrays