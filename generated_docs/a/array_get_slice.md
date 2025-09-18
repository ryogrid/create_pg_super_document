# array_get_slice

## Location
src/backend/utils/adt/arrayfuncs.c: 2030 - 2200

## Overview
Extracts a slice (subarray) from an array using upper and lower bounds for each dimension, creating a new array structure containing the specified range of elements.

## Definition
```c
Datum array_get_slice(Datum arraydatum,
                     int nSubscripts,
                     int *upperIndx,
                     int *lowerIndx,
                     bool *upperProvided,
                     bool *lowerProvided,
                     int arraytyplen,
                     int elmlen,
                     bool elmbyval,
                     char elmalign)
```

## Detailed Description
The `array_get_slice` function creates a new array containing a specified slice of an existing array. It handles both ordinary varlena arrays and fixed-length arrays (though fixed-length array slicing is currently not implemented and raises an error). The function accepts upper and lower bounds for each dimension, with optional provision flags indicating which bounds are explicitly specified. Missing bounds are replaced with corresponding array limits. The resulting slice has lower bounds reset to 1 for all dimensions. The function performs bounds checking, truncates slices that exceed array limits, and returns empty arrays for invalid ranges.

## Parameters / Member Variables
- `arraydatum`: The source array object (must not be NULL)
- `nSubscripts`: Number of subscripts supplied (must be same for upper/lower bounds)
- `upperIndx[]`: Array of upper subscript values for each dimension
- `lowerIndx[]`: Array of lower subscript values for each dimension
- `upperProvided[]`: Boolean flags indicating which upper subscripts are explicitly provided
- `lowerProvided[]`: Boolean flags indicating which lower subscripts are explicitly provided
- `arraytyplen`: pg_type.typlen for the array type
- `elmlen`: pg_type.typlen for the array's element type
- `elmbyval`: pg_type.typbyval for the array's element type
- `elmalign`: pg_type.typalign for the array's element type

## Dependencies
- Functions called/Symbols referenced:
  - `DatumGetArrayTypeP`: Converts datum to ArrayType pointer
  - `ARR_NDIM`, `ARR_DIMS`, `ARR_LBOUND`, `ARR_ELEMTYPE`: Array metadata accessors
  - `ARR_DATA_PTR`, `ARR_NULLBITMAP`: Array data accessors
  - [construct_empty_array](../c/construct_empty_array.md): Creates empty array for invalid slices
  - [mda_get_range](../m/mda_get_range.md): Calculates span dimensions from bounds
  - [array_slice_size](array_slice_size.md): Calculates required memory for slice data
  - `ArrayGetNItems`: Calculates total number of items
  - `ARR_OVERHEAD_WITHNULLS`, `ARR_OVERHEAD_NONULLS`: Array header size calculations
  - [array_extract_slice](array_extract_slice.md): Extracts actual slice data into new array
  - `SET_VARSIZE`: Sets variable-length header size
- Called from (representative examples):
  - [array_subscript_fetch_slice](array_subscript_fetch_slice.md): Array slice subscripting operations
  - [trim_array](../t/trim_array.md): Array trimming operations
  - [array_subscript_fetch_old_slice](array_subscript_fetch_old_slice.md): Legacy slice operations

## Notes and Other Information
- Always returns a valid Datum (never NULL), creating empty arrays for invalid ranges
- Fixed-length array slicing is not implemented and throws a FEATURE_NOT_SUPPORTED error
- Silently truncates slices that exceed current array limits to fit within bounds
- Resulting slice arrays always have lower bounds set to 1 regardless of source array bounds
- Assumes it's safe to modify the provided subscript arrays (lowerIndx and upperIndx)
- Subscript arrays must be of size MAXDIM even when nSubscripts is smaller
- Preserves null bitmap structure from source array in the result
- Critical function for PostgreSQL's array slicing infrastructure
- Located in `src/backend/utils/adt/arrayfuncs.c` at lines 2030-2200