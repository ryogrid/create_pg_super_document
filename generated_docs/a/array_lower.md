# array_lower

## Location
[src/backend/utils/adt/arrayfuncs.c:1706-1732](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L1706-L1732)

## Overview
Returns the lower bound of a specified dimension for a PostgreSQL array, providing access to the starting index of that dimension.

## Definition
```c
Datum array_lower(PG_FUNCTION_ARGS)
```

## Detailed Description
The `array_lower` function retrieves the lower bound (starting index) of a specific dimension in a PostgreSQL array. It takes two arguments: the array and the dimension number (1-based indexing). The function performs validation checks on both the array structure and the requested dimension number before returning the lower bound value. PostgreSQL arrays can have custom lower bounds (not necessarily starting at 1), making this function essential for proper array indexing.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_ANY_ARRAY_P` - macro to extract array argument
  - `PG_GETARG_INT32` - macro to extract integer argument (dimension number)
  - `AARR_NDIM` - macro to get number of dimensions from array header
  - `AARR_LBOUND` - macro to get lower bounds array from array header
  - `AnyArrayType` - generic array type structure
  - `MAXDIM` - maximum allowed array dimensions constant
  - `PG_RETURN_INT32` - macro to return 32-bit integer result
  - `PG_RETURN_NULL` - macro to return NULL value
- Called from (representative examples):
  - SQL queries using `array_lower()` function
  - Array processing routines requiring dimension bounds information

## Notes and Other Information
- Uses 1-based indexing for dimension numbers (dimension 1 is the first dimension)
- Returns NULL for invalid arrays (dimension count ≤ 0 or > MAXDIM)
- Returns NULL for invalid dimension requests (≤ 0 or > array's actual dimension count)
- PostgreSQL arrays can have custom lower bounds, not necessarily starting at 1
- Part of PostgreSQL's array introspection function suite
- Defined in src/backend/utils/adt/arrayfuncs.c:1706-1732
- Essential for proper array indexing when arrays have non-standard bounds

## Simplified Source

```c
Datum array_lower(PG_FUNCTION_ARGS) {
    // Get array and requested dimension number
    AnyArrayType *v = PG_GETARG_ANY_ARRAY_P(0);
    int reqdim = PG_GETARG_INT32(1);

    // Validate array structure
    if (AARR_NDIM(v) <= 0 || AARR_NDIM(v) > MAXDIM)
        PG_RETURN_NULL();

    // Validate requested dimension is within bounds
    if (reqdim <= 0 || reqdim > AARR_NDIM(v))
        PG_RETURN_NULL();

    // Get lower bounds array and return the requested dimension's lower bound
    int *lb = AARR_LBOUND(v);
    int result = lb[reqdim - 1];  // Convert 1-based to 0-based indexing

    PG_RETURN_INT32(result);
}
```