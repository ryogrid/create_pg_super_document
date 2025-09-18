# array_prepend

## Location
src/backend/utils/adt/array_userfuncs.c: 176 - 239

## Overview
PostgreSQL function that pushes an element onto the front of a one-dimensional array, extending the array by one element at the beginning.

## Definition
```c
Datum array_prepend(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL array_prepend functionality, which takes a new element and an existing array, then prepends the element to the beginning of the array. The function is designed to work with one-dimensional arrays only and handles both null and non-null input arrays gracefully.

Unlike array_append, array_prepend takes its arguments in reverse order: the element to prepend is the first argument, and the target array is the second argument. The implementation uses PostgreSQL's expanded array representation for efficiency and includes special handling to maintain the original array's lower bound after insertion.

The function includes overflow protection when calculating the new element index and provides clear error messages for invalid input. After insertion, it readjusts the result's lower bound to match the original array's lower bound, as expected for prepend operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - Argument 0: The element to prepend (can be null)
  - Argument 1: The target array (can be null)

## Dependencies
- Functions called/Symbols referenced:
  - PG_ARGISNULL
  - PG_GETARG_DATUM
  - [fetch_array_arg_replace_nulls](../f/fetch_array_arg_replace_nulls.md)
  - [pg_sub_s32_overflow](../p/pg_sub_s32_overflow.md)
  - [array_set_element](array_set_element.md)
  - EOHPGetRWDatum
  - PG_RETURN_DATUM
- Called from (representative examples):
  - SQL array_prepend() function calls
  - Internal PostgreSQL array operations

## Notes and Other Information
- Only works with empty arrays (0 dimensions) or one-dimensional arrays
- Arguments are in reverse order compared to array_append (element first, array second)
- Provides overflow protection when calculating array indices using subtraction
- Uses expanded array headers for efficient array manipulation
- Maintains the original array's lower bound after prepending
- Returns a new array datum rather than modifying the input in-place
- Handles null elements gracefully by preserving null values in the result array
- Part of PostgreSQL's array manipulation function suite