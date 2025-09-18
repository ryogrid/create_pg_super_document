# array_append

## Location
src/backend/utils/adt/array_userfuncs.c: 123 - 175

## Overview
PostgreSQL function that pushes an element onto the end of a one-dimensional array, extending the array by one element.

## Definition
```c
Datum array_append(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL array_append functionality, which takes an existing array and appends a new element to its end. The function is designed to work with one-dimensional arrays only and handles both null and non-null input arrays gracefully.

The implementation uses PostgreSQL's expanded array representation for efficiency. When given a null array, it creates a new single-element array. For existing arrays, it calculates the appropriate index for the new element and uses the array_set_element function to perform the actual insertion.

The function includes overflow protection when calculating the new element index and provides clear error messages for invalid input (such as multi-dimensional arrays).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - Argument 0: The input array (can be null)
  - Argument 1: The element to append (can be null)

## Dependencies
- Functions called/Symbols referenced:
  - [fetch_array_arg_replace_nulls](../f/fetch_array_arg_replace_nulls.md)
  - PG_ARGISNULL
  - PG_GETARG_DATUM
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md)
  - [array_set_element](array_set_element.md)
  - EOHPGetRWDatum
  - PG_RETURN_DATUM
- Called from (representative examples):
  - SQL array_append() function calls
  - Internal PostgreSQL array operations

## Notes and Other Information
- Only works with empty arrays (0 dimensions) or one-dimensional arrays
- Provides overflow protection when calculating array indices
- Uses expanded array headers for efficient array manipulation
- Returns a new array datum rather than modifying the input in-place
- Handles null elements gracefully by preserving null values in the result array
- Part of PostgreSQL's array manipulation function suite