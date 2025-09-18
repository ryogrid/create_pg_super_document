# get_func_support

## Location
src/backend/utils/cache/lsyscache.c: 1858 - 1884

## Overview
Returns the support function OID associated with a given function, or InvalidOid if there is no support function defined.

## Definition
```c
RegProcedure get_func_support(Oid funcid)
```

## Detailed Description
This function retrieves the support function OID for a PostgreSQL function from the system catalog. Support functions are special functions that provide additional information or optimization hints for the query planner when dealing with specific functions. They are typically used for functions that can benefit from custom selectivity estimation, index optimization, or other planner enhancements. The function performs a system cache lookup on the pg_proc catalog to retrieve the prosupport field.

## Parameters / Member Variables
- `funcid`: The OID of the function for which to retrieve the support function

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_proc
  - InvalidOid
- Called from (representative examples):
  - [find_window_run_conditions](../f/find_window_run_conditions.md)
  - [get_index_clause_from_support](get_index_clause_from_support.md)
  - [optimize_window_clauses](../o/optimize_window_clauses.md)
  - [function_selectivity](../f/function_selectivity.md)

## Notes and Other Information
- Returns InvalidOid if the function is not found in the system cache or if no support function is defined
- Support functions are an advanced feature used primarily for custom data types and operators to provide planner hints
- Unlike get_func_leakproof, this function does not raise an error for missing functions, instead returning InvalidOid
- Support functions are used by the query planner for selectivity estimation and optimization decisions