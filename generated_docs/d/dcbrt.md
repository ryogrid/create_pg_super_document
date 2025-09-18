# dcbrt

## Location
src/backend/utils/adt/float.c: 1463 - 1481

## Overview
The dcbrt function computes the cube root of a double-precision floating-point number with error handling for overflow and underflow conditions.

## Definition
```c
Datum dcbrt(PG_FUNCTION_ARGS)
```

## Detailed Description
The dcbrt function implements the cube root operation for double-precision floating-point numbers (float8). Unlike square root, cube root is defined for negative numbers, so the function accepts any real number input. The function includes error handling for:
- Overflow conditions (when result is infinite but input is finite)
- Underflow conditions (when result is zero but input is non-zero)

The function uses the standard C library cbrt() function but adds PostgreSQL-specific overflow/underflow detection and error reporting.

## Parameters / Member Variables
- `arg1`: The input double-precision floating-point number for which to calculate the cube root (can be any real number)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (macro to extract float8 argument)
  - cbrt (standard C math library function)
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
- Unlike dsqrt, this function accepts negative inputs since cube root is defined for all real numbers
- Implements overflow/underflow detection beyond what the standard cbrt() provides
- Follows standard PostgreSQL function conventions for SQL-callable functions