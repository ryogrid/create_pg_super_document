# array_set_element

## Location
[src/backend/utils/adt/arrayfuncs.c:2201-2500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L2201-L2500)

## Overview
Sets the value of a single array element at specified subscripts, creating a new array with the modified element while preserving the original array structure.

## Definition
```c
Datum array_set_element(Datum arraydatum,
                       int nSubscripts,
                       int *indx,
                       Datum dataValue,
                       bool isNull,
                       int arraytyplen,
                       int elmlen,
                       bool elmbyval,
                       char elmalign)
```

## Detailed Description
The `array_set_element` function creates a new array identical to the input array except for one modified element at the specified subscripts. It handles three array types: fixed-length arrays (1-dimensional, 0-based, no extension allowed), expanded arrays (delegated to `array_set_element_expanded`), and ordinary varlena arrays. For one-dimensional arrays, it supports array extension by assigning to positions outside the existing range, filling gaps with NULLs. Multi-dimensional arrays cannot be extended. The function performs comprehensive bounds checking, handles overflow prevention, manages null bitmaps, and creates optimally-sized result arrays. Unlike fetch operations that return NULL for invalid subscripts, this function throws errors for assignment to invalid positions.

## Parameters / Member Variables
- `arraydatum`: The source array object (must not be NULL)
- `nSubscripts`: Number of subscripts supplied in the indx array
- `indx[]`: Array of subscript values specifying the target element position
- `dataValue`: The datum value to be inserted at the specified position
- `isNull`: Boolean indicating whether dataValue is NULL
- `arraytyplen`: pg_type.typlen for the array type (>0 for fixed-length)
- `elmlen`: pg_type.typlen for the array's element type
- `elmbyval`: pg_type.typbyval for the array's element type
- `elmalign`: pg_type.typalign for the array's element type

## Dependencies
- Functions called/Symbols referenced:
  - `[ArrayCastAndSet](../A/ArrayCastAndSet.md)`: Sets element value in fixed-length arrays
  - `PG_DETOAST_DATUM`: Detoasts varlena elements before insertion
  - `VARATT_IS_EXTERNAL_EXPANDED`: Checks for expanded array format
  - [array_set_element_expanded](array_set_element_expanded.md): Handles expanded array assignments
  - `DatumGetArrayTypeP`: Converts datum to ArrayType pointer
  - `ARR_NDIM`, `ARR_DIMS`, `ARR_LBOUND`, `ARR_ELEMTYPE`: Array metadata
  - [construct_md_array](../c/construct_md_array.md): Creates new arrays from empty arrays
  - `[ArrayGetNItems](../A/ArrayGetNItems.md)`, `ArrayCheckBounds`: Array validation functions
  - `ARR_OVERHEAD_WITHNULLS`, `ARR_OVERHEAD_NONULLS`: Size calculations
  - `[ArrayGetOffset](../A/ArrayGetOffset.md)`: Calculates linear offset from subscripts
  - `[array_seek](array_seek.md)`, `array_get_isnull`: Element access functions
  - `att_addlength_datum`, `att_align_nominal`: Data size/alignment
  - `[array_set_isnull](array_set_isnull.md)`, `array_bitmap_copy`: Null bitmap management
- Called from (representative examples):
  - [array_set](array_set.md): Array assignment operations
  - [array_subscript_assign](array_subscript_assign.md): Subscripted assignment operations
  - [array_append](array_append.md), `array_prepend`: Array modification functions

## Notes and Other Information
- Always returns a new array; never modifies the original (except for writable expanded arrays)
- Supports 1D array extension: assignments beyond bounds extend the array with NULLs filling gaps
- Multi-dimensional arrays cannot be extended; out-of-bounds assignment raises errors
- Fixed-length arrays: 1D only, 0-based indexing, no extension, no NULL assignments allowed
- Handles overflow protection using `pg_sub_s32_overflow` and `pg_add_s32_overflow`
- Maximum array size enforced by `MaxArraySize` constant
- Empty arrays (ndim=0) are converted to new arrays with specified dimensions
- Comprehensive null bitmap management preserves and updates NULL element tracking
- Critical function for PostgreSQL's array assignment infrastructure
- Located in `src/backend/utils/adt/arrayfuncs.c` at lines 2201-2500