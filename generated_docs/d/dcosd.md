# dcosd

## Location
src/backend/utils/adt/float.c: 2311 - 2365

## Overview
The `dcosd` function is a PostgreSQL built-in function that returns the cosine of an angle specified in degrees, handling input validation and range reduction.

## Definition
```c
Datum dcosd(PG_FUNCTION_ARGS)
```

## Detailed Description
The `dcosd` function implements the cosine function for degree-based input in PostgreSQL's SQL interface. It provides robust handling of edge cases and follows POSIX specifications:

1. **Input validation**: Checks for NaN (returns NaN) and infinite values (throws error)
2. **Range reduction**: Reduces any input angle to the [0, 90] degree range using trigonometric identities:
   - Uses modulo 360° to handle angles beyond one full rotation
   - Exploits cosine's even symmetry: cos(-x) = cos(x)
   - Uses period symmetry: cos(360° - x) = cos(x)  
   - Uses supplementary angle identity: cos(180° - x) = -cos(x)
3. **Computation**: Delegates to the optimized `cosd_q1` function for first quadrant calculation
4. **Error handling**: Checks for overflow conditions in the result

## Parameters / Member Variables
- `arg1`: Input angle in degrees (float8/double precision)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (argument retrieval)
  - isnan, isinf (special value checks)
  - get_float8_nan (NaN handling)
  - INIT_DEGREE_CONSTANTS (initialization)
  - cosd_q1 (first quadrant cosine calculation)
  - float_overflow_error (error reporting)
- Called from: (No direct callers - SQL-callable function)

## Notes and Other Information
- This is a SQL-callable function using PostgreSQL's function call convention
- Follows POSIX specification for handling special values (NaN, infinity)
- Implements comprehensive range reduction to leverage the high-precision `cosd_q1` function
- Part of PostgreSQL's degree-based trigonometric function family
- Returns double precision floating point results
- Handles overflow conditions by calling appropriate error reporting functions