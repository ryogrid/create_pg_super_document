# array_cat

## Location
[src/backend/utils/adt/array_userfuncs.c:240-478](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L240-L478)

## Overview
PostgreSQL function that concatenates two n-dimensional arrays to form an n-dimensional array, or pushes an (n-1)-dimensional array onto the end of an n-dimensional array.

## Definition
```c
Datum array_cat(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL array concatenation functionality (|| operator), which combines two arrays into a single array. The function handles multiple concatenation scenarios based on the dimensionality of the input arrays:

1. **Same dimensions (ndims1 == ndims2)**: Concatenates arrays along the first dimension
2. **First array has one less dimension (ndims1 == ndims2 - 1)**: Inserts the first array as an element at the front of the second array
3. **Second array has one less dimension (ndims1 == ndims2 + 1)**: Appends the second array as an element at the end of the first array
4. **Empty array handling**: Returns the non-empty array when one input is empty

The function performs extensive validation to ensure arrays are compatible for concatenation, including element type matching and dimensional consistency. It efficiently handles null bitmaps, data copying, and memory allocation for the result array.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - Argument 0: First array to concatenate (can be null)
  - Argument 1: Second array to concatenate (can be null)

## Dependencies
- Functions called/Symbols referenced:
  - PG_ARGISNULL
  - PG_GETARG_ARRAYTYPE_P
  - PG_RETURN_ARRAYTYPE_P
  - PG_RETURN_NULL
  - ARR_ELEMTYPE
  - ARR_NDIM
  - ARR_LBOUND
  - ARR_DIMS
  - ARR_DATA_PTR
  - ARR_NULLBITMAP
  - ARR_HASNULL
  - ARR_SIZE
  - ARR_DATA_OFFSET
  - ARR_OVERHEAD_WITHNULLS
  - ARR_OVERHEAD_NONULLS
  - [ArrayGetNItems](../A/ArrayGetNItems.md)
  - [ArrayCheckBounds](../A/ArrayCheckBounds.md)
  - [array_bitmap_copy](array_bitmap_copy.md)
  - SET_VARSIZE
  - [format_type_be](../f/format_type_be.md)
  - [palloc](../p/palloc.md)
  - [palloc0](../p/palloc0.md)
  - memcpy
- Called from (representative examples):
  - SQL array concatenation operations (||)
  - Internal PostgreSQL array operations

## Notes and Other Information
- Supports concatenation of multi-dimensional arrays with complex dimension matching rules
- Provides comprehensive error messages for incompatible array combinations
- Handles null arrays gracefully by treating concatenation with null as a no-op
- Efficiently manages memory allocation and null bitmap copying
- Validates element type compatibility and dimensional constraints
- Supports both arrays with and without null elements
- Critical component of PostgreSQL's array manipulation capabilities
- Returns the result in the standard PostgreSQL ArrayType format