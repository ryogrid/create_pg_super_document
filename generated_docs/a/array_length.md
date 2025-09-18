# array_length

## Location
src/backend/utils/adt/arrayfuncs.c: 1763 - 1789

## Overview
Returns the length (number of elements) of a specified dimension for a PostgreSQL array, providing essential size information for array processing.

## Definition
```c
Datum array_length(PG_FUNCTION_ARGS)
```

## Detailed Description
The `array_length` function retrieves the length (size) of a specific dimension in a PostgreSQL array. It takes two arguments: the array and the dimension number (1-based indexing). The function directly returns the dimension size from the array's metadata without needing to calculate it from bounds. It performs validation checks on both the array structure and the requested dimension number before returning the length value.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Array argument accessed via `PG_GETARG_ANY_ARRAY_P(0)` - the input array to examine
  - Dimension number accessed via `PG_GETARG_INT32(1)` - the dimension to query (1-based)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_ANY_ARRAY_P` - macro to extract array argument
  - `PG_GETARG_INT32` - macro to extract integer argument (dimension number)
  - `AARR_NDIM` - macro to get number of dimensions from array header
  - `AARR_DIMS` - macro to get dimension sizes from array header
  - `AnyArrayType` - generic array type structure
  - `MAXDIM` - maximum allowed array dimensions constant
  - `PG_RETURN_INT32` - macro to return 32-bit integer result
  - `PG_RETURN_NULL` - macro to return NULL value
- Called from (representative examples):
  - SQL queries using `array_length()` function
  - [trim_array](../t/trim_array.md) function for array manipulation operations
  - Array processing routines requiring dimension size information

## Notes and Other Information
- Uses 1-based indexing for dimension numbers (dimension 1 is the first dimension)
- Returns the actual number of elements in the specified dimension
- Returns NULL for invalid arrays (dimension count ≤ 0 or > MAXDIM)
- Returns NULL for invalid dimension requests (≤ 0 or > array's actual dimension count)
- More efficient than calculating length from upper and lower bounds
- Part of PostgreSQL's array introspection function suite
- Defined in src/backend/utils/adt/arrayfuncs.c:1763-1789
- Used internally by array manipulation functions like `trim_array`