# dcosd

## Location
[src/backend/utils/adt/float.c:2311-2365](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2311-L2365)

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
  - [get_float8_nan](../g/get_float8_nan.md) (NaN handling)
  - INIT_DEGREE_CONSTANTS (initialization)
  - [cosd_q1](../c/cosd_q1.md) (first quadrant cosine calculation)
  - [float_overflow_error](../f/float_overflow_error.md) (error reporting)
- Called from: (No direct callers - SQL-callable function)

## Notes and Other Information
- This is a SQL-callable function using PostgreSQL's function call convention
- Follows POSIX specification for handling special values (NaN, infinity)
- Implements comprehensive range reduction to leverage the high-precision `cosd_q1` function
- Part of PostgreSQL's degree-based trigonometric function family
- Returns double precision floating point results
- Handles overflow conditions by calling appropriate error reporting functions

## Simplified Source

```c
Datum dcosd(PG_FUNCTION_ARGS) {
    float8 arg1 = PG_GETARG_FLOAT8(0);
    float8 result;
    int sign = 1;

    // Handle special values per POSIX spec
    if (isnan(arg1))
        PG_RETURN_FLOAT8(get_float8_nan());

    if (isinf(arg1))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                       errmsg("input is out of range")));

    INIT_DEGREE_CONSTANTS();

    // Range reduction: reduce input to [0, 90] degrees
    arg1 = fmod(arg1, 360.0);           // Handle full rotations

    if (arg1 < 0.0) {
        arg1 = -arg1;                   // cos(-x) = cos(x)
    }

    if (arg1 > 180.0) {
        arg1 = 360.0 - arg1;            // cos(360-x) = cos(x)
    }

    if (arg1 > 90.0) {
        arg1 = 180.0 - arg1;            // cos(180-x) = -cos(x)
        sign = -sign;
    }

    // Calculate using optimized first quadrant function
    result = sign * cosd_q1(arg1);

    if (unlikely(isinf(result)))
        float_overflow_error();

    PG_RETURN_FLOAT8(result);
}
```