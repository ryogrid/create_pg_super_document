# dsqrt

## Location
[src/backend/utils/adt/float.c:1439-1462](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1439-L1462)

## Overview
The dsqrt function computes the square root of a double-precision floating-point number with comprehensive error handling and domain validation.

## Definition
```c
Datum dsqrt(PG_FUNCTION_ARGS)
```

## Detailed Description
The dsqrt function implements the square root operation for double-precision floating-point numbers (float8). It includes robust error handling for domain errors (negative inputs), overflow conditions, and underflow conditions. The function ensures that:
- Negative inputs result in an appropriate SQL error rather than NaN
- Overflow conditions (when result is infinite but input is finite) are detected and reported
- Underflow conditions (when result is zero but input is non-zero) are detected and reported

The function uses the standard C library sqrt() function but adds PostgreSQL-specific error handling.

## Parameters / Member Variables
- `arg1`: The input double-precision floating-point number for which to calculate the square root (must be non-negative)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (macro to extract float8 argument)
  - ereport (PostgreSQL error reporting function)
  - [errcode](../e/errcode.md) (PostgreSQL error code function)
  - [errmsg](../e/errmsg.md) (PostgreSQL error message function)
  - sqrt (standard C math library function)
  - isinf (standard C math library function to check for infinity)
  - unlikely (compiler hint macro)
  - [float_overflow_error](../f/float_overflow_error.md) (PostgreSQL float overflow error handler)
  - [float_underflow_error](../f/float_underflow_error.md) (PostgreSQL float underflow error handler)
  - PG_RETURN_FLOAT8 (macro to return float8 result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's floating-point arithmetic operations
- Located in src/backend/utils/adt/float.c which contains various floating-point utility functions
- Uses specific SQL error code ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION for domain errors
- Implements comprehensive overflow/underflow detection beyond what the standard sqrt() provides
- Follows standard PostgreSQL function conventions for SQL-callable functions

## Simplified Source

```c
Datum dsqrt(PG_FUNCTION_ARGS) {
    // Extract input argument
    float8 arg1 = PG_GETARG_FLOAT8(0);

    // Check for negative input (domain error)
    if (arg1 < 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
                 errmsg("cannot take square root of a negative number")));

    // Compute square root
    float8 result = sqrt(arg1);

    // Check for overflow and underflow
    if (unlikely(isinf(result)) && !isinf(arg1))
        float_overflow_error();
    if (unlikely(result == 0.0) && arg1 != 0.0)
        float_underflow_error();

    PG_RETURN_FLOAT8(result);
}
```