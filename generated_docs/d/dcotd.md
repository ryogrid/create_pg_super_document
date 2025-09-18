# dcotd

## Location
src/backend/utils/adt/float.c: 2366 - 2431

## Overview
The `dcotd` function is a PostgreSQL built-in function that returns the cotangent of an angle specified in degrees, handling input validation, range reduction, and special cotangent calculations.

## Definition
```c
Datum dcotd(PG_FUNCTION_ARGS)
```

## Detailed Description
The `dcotd` function implements the cotangent function for degree-based input in PostgreSQL's SQL interface. It provides robust handling of edge cases and follows POSIX specifications:

1. **Input validation**: Checks for NaN (returns NaN) and infinite values (throws error)
2. **Range reduction**: Reduces any input angle to the [0, 90] degree range using cotangent identities:
   - Uses modulo 360° to handle angles beyond one full rotation
   - Exploits cotangent's odd symmetry: cot(-x) = -cot(x)
   - Uses period properties: cot(360° - x) = -cot(x)
   - Uses supplementary angle identity: cot(180° - x) = -cot(x)
3. **Computation**: Calculates cotangent as cos(x)/sin(x) using the optimized `cosd_q1` and `sind_q1` functions, then normalizes by `cot_45` (cotangent of 45°)
4. **Special handling**: Forces minus zero results to plain zero for portability
5. **No overflow check**: Deliberately allows infinite results (e.g., cot(0°) = ∞)

## Parameters / Member Variables
- `arg1`: Input angle in degrees (float8/double precision)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (argument retrieval)
  - isnan, isinf (special value checks)
  - get_float8_nan (NaN handling)
  - INIT_DEGREE_CONSTANTS (initialization)
  - cosd_q1, sind_q1 (first quadrant cosine and sine calculations)
  - cot_45 (global constant for cotangent of 45°)
- Called from: (No direct callers - SQL-callable function)

## Notes and Other Information
- This is a SQL-callable function using PostgreSQL's function call convention
- Follows POSIX specification for handling special values (NaN, infinity)
- Uses a `volatile` variable `cot_arg1` to ensure proper floating-point behavior
- Deliberately allows infinite results for angles like 0° where cotangent is mathematically infinite
- Forces minus zero to plain zero for better portability across different platforms
- Part of PostgreSQL's degree-based trigonometric function family
- Returns double precision floating point results