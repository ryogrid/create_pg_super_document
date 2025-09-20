# get_func_trftypes

## Location
[src/backend/utils/fmgr/funcapi.c:1475-1521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L1475-L1521)

## Overview
Retrieves the transformed types associated with a PostgreSQL function from its pg_proc catalog entry.

## Definition

```c
struct_array() since the array data is just going to look like
		 * a C array of values.
		 */
		arr = DatumGetArrayTypeP(protrftypes);
```
## Detailed Description
This function extracts the protrftypes array from a function's pg_proc tuple, which contains the OIDs of data types that have been transformed for use with the function. Transformed types are used in PostgreSQL's type transformation system, where certain data types can be automatically converted or adapted when used as function arguments or return values.

The function validates the structure of the protrftypes array to ensure it's a well-formed 1-D array of OIDs without null values. If no transformed types are present, it returns 0 and leaves the output parameter unchanged.

The returned array is palloc'd and becomes the caller's responsibility to manage.

## Parameters / Member Variables
- : HeapTuple pointing to the pg_proc catalog entry for the function
- : Output parameter receiving palloc'd array of transformed type OIDs (only set if types exist)

## Dependencies
- Functions called/Symbols referenced:
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - DatumGetArrayTypeP
  - ARR_DIMS, ARR_NDIM, ARR_HASNULL, ARR_ELEMTYPE, ARR_DATA_PTR
  - [palloc](../p/palloc.md), memcpy
  - Anum_pg_proc_protrftypes
- Called from (representative examples):
  - [print_function_trftypes](../p/print_function_trftypes.md)
  - TypeFuncClass (referenced in funcapi.h)

## Notes and Other Information
- Returns the number of transformed types (0 if none)
- Only allocates and populates p_trftypes if transformed types are present
- Validates array structure and reports errors for malformed catalog data
- Part of PostgreSQL's type transformation framework for automatic type conversions
- Used primarily by system utilities that need to display or analyze function type transformations
- Transformed types are relatively specialized and not used by all functions