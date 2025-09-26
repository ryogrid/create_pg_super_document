# accumArrayResultArr

## Location
[src/backend/utils/adt/arrayfuncs.c:5538-5690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L5538-L5690)

## Overview
Accumulates one sub-array into an ArrayBuildStateArr structure, building up data for creating a multi-dimensional array result.

## Definition

```c
ArrayBuildStateArr *
accumArrayResultArr(ArrayBuildStateArr *astate,
					Datum dvalue, bool disnull,
					Oid array_type,
					MemoryContext rcontext)
```
## Detailed Description
This function is the core accumulation function for building arrays from arrays. It takes an input sub-array and adds it to the working state, ensuring all sub-arrays have consistent dimensionality. The function handles memory management, including dynamic expansion of data and null bitmap storage as needed.

On the first call (when astate is NULL), it initializes the working state and establishes the dimensionality pattern that all subsequent inputs must match. For subsequent calls, it validates that new inputs match the established pattern and accumulates their data.

The function manages both the actual array data and null bitmaps, handling cases where some sub-arrays have nulls and others don't. It automatically expands storage as needed using power-of-2 growth for efficiency.

## Parameters / Member Variables
- : Working ArrayBuildStateArr state (can be NULL on first call, will be created)
- : Datum containing the new sub-array to append
- : Boolean indicating if the sub-array value is null (causes error if true)
- : OID of the array type (must be valid varlena array type)
- : Memory context for keeping working state

## Dependencies
- Functions called/Symbols referenced:
  - [initArrayResultArr](../i/initArrayResultArr.md)
  - DatumGetArrayTypeP
  - [ArrayGetNItems](../A/ArrayGetNItems.md)
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md)
  - [array_bitmap_copy](array_bitmap_copy.md)
  - [repalloc](../r/repalloc.md)
  - ARR_NDIM, ARR_DIMS, ARR_LBOUND, ARR_DATA_PTR
  - ARR_HASNULL, ARR_NULLBITMAP
- Called from (representative examples):
  - [array_agg_array_transfn](array_agg_array_transfn.md)
  - [accumArrayResultAny](accumArrayResultAny.md)

## Notes and Other Information
- **Error conditions**: 
  - Null sub-arrays are not allowed and will cause an error
  - Empty arrays cannot be accumulated
  - All sub-arrays must have identical dimensionality
  - Exceeding MAXDIM dimensions will cause an error
- **Memory management**: Uses power-of-2 growth for both data storage and null bitmap storage
- **Dimensionality**: The output array will have N+1 dimensions where N is the dimensionality of input sub-arrays
- **Performance**: Automatically handles memory expansion and detoasting of input arrays
- **Null handling**: Retrospectively handles null bitmaps when the first array with nulls is encountered
- Part of the three-function API: initArrayResultArr/accumArrayResultArr/makeArrayResultArr