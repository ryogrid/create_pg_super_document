# array_upper

## Location
[src/backend/utils/adt/arrayfuncs.c:1733-1762](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L1733-L1762)

## Overview
Returns the upper bound of a specified dimension for a PostgreSQL array, providing access to the ending index of that dimension.

## Definition
```c
Datum array_upper(PG_FUNCTION_ARGS)
```

## Detailed Description
The `array_upper` function retrieves the upper bound (ending index) of a specific dimension in a PostgreSQL array. It calculates the upper bound by adding the dimension size to the lower bound and subtracting 1. The function takes two arguments: the array and the dimension number (1-based indexing). It performs validation checks on both the array structure and the requested dimension number before returning the calculated upper bound value.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_ANY_ARRAY_P` - macro to extract array argument
  - `PG_GETARG_INT32` - macro to extract integer argument (dimension number)
  - `AARR_NDIM` - macro to get number of dimensions from array header
  - `AARR_LBOUND` - macro to get lower bounds array from array header
  - `AARR_DIMS` - macro to get dimension sizes from array header
  - `AnyArrayType` - generic array type structure
  - `MAXDIM` - maximum allowed array dimensions constant
  - `PG_RETURN_INT32` - macro to return 32-bit integer result
  - `PG_RETURN_NULL` - macro to return NULL value
- Called from (representative examples):
  - SQL queries using `array_upper()` function
  - Array processing routines requiring dimension bounds information

## Notes and Other Information
- Uses 1-based indexing for dimension numbers (dimension 1 is the first dimension)
- Calculates upper bound as: dimension_size + lower_bound - 1
- Returns NULL for invalid arrays (dimension count ≤ 0 or > MAXDIM)
- Returns NULL for invalid dimension requests (≤ 0 or > array's actual dimension count)
- Companion function to `array_lower` for complete dimension boundary information
- Part of PostgreSQL's array introspection function suite
- Defined in src/backend/utils/adt/arrayfuncs.c:1733-1762
- Essential for determining valid index ranges for array elements

## Simplified Source

```c
Datum array_upper(PG_FUNCTION_ARGS) {
    // Get array and requested dimension number
    AnyArrayType *v = PG_GETARG_ANY_ARRAY_P(0);
    int reqdim = PG_GETARG_INT32(1);

    // Validate array structure
    if (AARR_NDIM(v) <= 0 || AARR_NDIM(v) > MAXDIM)
        PG_RETURN_NULL();

    // Validate requested dimension is within bounds
    if (reqdim <= 0 || reqdim > AARR_NDIM(v))
        PG_RETURN_NULL();

    // Get dimension info: lower bounds and sizes
    int *lb = AARR_LBOUND(v);
    int *dimv = AARR_DIMS(v);

    // Calculate upper bound: size + lower_bound - 1
    int result = dimv[reqdim - 1] + lb[reqdim - 1] - 1;

    PG_RETURN_INT32(result);
}
```