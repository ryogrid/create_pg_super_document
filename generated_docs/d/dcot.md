# dcot

## Location
src/backend/utils/adt/float.c: 1903 - 1930

## Overview
The dcot function computes the cotangent of a floating-point argument given in radians, returning the reciprocal of the tangent value.

## Definition
```c
Datum dcot(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL COT function for PostgreSQL, computing the cotangent of the input value. Cotangent is mathematically defined as the reciprocal of tangent (cot(x) = 1/tan(x)). The implementation first computes the tangent using the standard C library tan() function, then takes the reciprocal. The function includes error handling similar to other trigonometric functions, checking for infinite inputs and NaN values. Notably, it does not check for overflow when computing the reciprocal because cot(0) legitimately equals infinity.

## Parameters / Member Variables
- `arg1`: The floating-point input value in radians for which to compute the cotangent

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8
  - isnan
  - get_float8_nan
  - tan (standard C library function)
  - isinf
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns NaN if the input is NaN, per POSIX specification
- Computes cotangent as the reciprocal of tangent (1/tan(x))
- Does not check for overflow in the reciprocal calculation since cot(0) = ∞ is mathematically valid
- Includes error checking for infinite inputs similar to other trigonometric functions
- Uses errno-based error detection following the same pattern as dcos()
- Located in src/backend/utils/adt/float.c:1903-1930