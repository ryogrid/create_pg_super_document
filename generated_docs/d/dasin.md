# dasin

## Location
src/backend/utils/adt/float.c: 1779 - 1809

## Overview
The dasin function implements PostgreSQL's inverse sine (arcsine) function, returning the arcsine of the input argument in radians.

## Definition
```c
Datum dasin(PG_FUNCTION_ARGS)
```

## Detailed Description
The dasin function is PostgreSQL's implementation of the inverse sine function (asin). It takes a single float8 argument and returns asin(arg1) in radians. The function implements strict domain checking as required by the mathematical definition of arcsine, which only accepts values in the range [-1, 1]. The principal branch of the inverse sine function maps values in the range [-1, 1] to values in the range [-π/2, π/2].

The function explicitly handles:
- NaN input (returns NaN per POSIX specification)
- Out-of-range input values (arg1 < -1.0 or arg1 > 1.0) - throws ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE
- Overflow conditions (throws float_overflow_error)

## Parameters / Member Variables
- `arg1`: The float8 input value for which to compute the arcsine, must be in range [-1, 1]

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (to extract input argument)
  - isnan (to check for NaN values)
  - get_float8_nan (to return NaN value)
  - ereport (PostgreSQL error reporting system)
  - [errcode](../e/errcode.md) (error code specification)
  - [errmsg](../e/errmsg.md) (error message specification)
  - asin (standard C library arcsine function)
  - isinf (to check for infinity values)
  - [float_overflow_error](../f/float_overflow_error.md) (PostgreSQL error handling)
- Called from: 
  - No direct references found in the codebase (likely called through SQL function dispatch)

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:1779-1809
- This function is part of PostgreSQL's floating-point trigonometric operations
- Follows POSIX specification for NaN handling
- Domain restrictions: arg1 must be in range [-1, 1]
- [Result](../R/Result.md) range: [-π/2, π/2] radians
- Uses ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE for domain violations
- The function follows PostgreSQL's standard function interface using PG_FUNCTION_ARGS and PG_RETURN_FLOAT8