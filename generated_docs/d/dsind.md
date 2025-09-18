# dsind

## Location
src/backend/utils/adt/float.c: 2432 - 2487

## Overview
The `dsind` function is a PostgreSQL built-in function that returns the sine of an angle specified in degrees, handling input validation and range reduction.

## Definition
```c
Datum dsind(PG_FUNCTION_ARGS)
```

## Detailed Description
The `dsind` function implements the sine function for degree-based input in PostgreSQL's SQL interface. It provides robust handling of edge cases and follows POSIX specifications:

1. **Input validation**: Checks for NaN (returns NaN) and infinite values (throws error)
2. **Range reduction**: Reduces any input angle to the [0, 90] degree range using trigonometric identities:
   - Uses modulo 360° to handle angles beyond one full rotation
   - Exploits sine's odd symmetry: sin(-x) = -sin(x)
   - Uses period symmetry: sin(360° - x) = -sin(x)
   - Uses supplementary angle identity: sin(180° - x) = sin(x)
3. **Computation**: Delegates to the optimized `sind_q1` function for first quadrant calculation
4. **Error handling**: Checks for overflow conditions in the result

## Parameters / Member Variables
- `arg1`: Input angle in degrees (float8/double precision)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (argument retrieval)
  - isnan, isinf (special value checks)
  - get_float8_nan (NaN handling)
  - INIT_DEGREE_CONSTANTS (initialization)
  - sind_q1 (first quadrant sine calculation)
  - float_overflow_error (error reporting)
- Called from: (No direct callers - SQL-callable function)

## Notes and Other Information
- This is a SQL-callable function using PostgreSQL's function call convention
- Follows POSIX specification for handling special values (NaN, infinity)
- Implements comprehensive range reduction to leverage the high-precision `sind_q1` function
- Part of PostgreSQL's degree-based trigonometric function family, complementing `dcosd`
- Returns double precision floating point results
- Handles overflow conditions by calling appropriate error reporting functions
- Uses the same range reduction pattern as `dcosd` but applies sine-specific trigonometric identities