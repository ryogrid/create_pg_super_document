# dacos

## Location
src/backend/utils/adt/float.c: 1748 - 1778

## Overview
The dacos function implements PostgreSQL's inverse cosine (arccosine) function, returning the arccosine of the input argument in radians.

## Definition
```c
Datum dacos(PG_FUNCTION_ARGS)
```

## Detailed Description
The dacos function is PostgreSQL's implementation of the inverse cosine function (acos). It takes a single float8 argument and returns acos(arg1) in radians. The function implements strict domain checking as required by the mathematical definition of arccosine, which only accepts values in the range [-1, 1]. The principal branch of the inverse cosine function maps values in the range [-1, 1] to values in the range [0, π].

The function explicitly handles:
- NaN input (returns NaN per POSIX specification)
- Out-of-range input values (arg1 < -1.0 or arg1 > 1.0) - throws ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE
- Overflow conditions (throws float_overflow_error)

## Parameters / Member Variables
- `arg1`: The float8 input value for which to compute the arccosine, must be in range [-1, 1]

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (to extract input argument)
  - isnan (to check for NaN values)
  - get_float8_nan (to return NaN value)
  - ereport (PostgreSQL error reporting system)
  - errcode (error code specification)
  - errmsg (error message specification)
  - acos (standard C library arccosine function)
  - isinf (to check for infinity values)
  - float_overflow_error (PostgreSQL error handling)
- Called from: 
  - No direct references found in the codebase (likely called through SQL function dispatch)

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:1748-1778
- This function is part of PostgreSQL's floating-point trigonometric operations
- Follows POSIX specification for NaN handling
- Domain restrictions: arg1 must be in range [-1, 1]
- Result range: [0, π] radians
- Uses ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE for domain violations
- The function follows PostgreSQL's standard function interface using PG_FUNCTION_ARGS and PG_RETURN_FLOAT8