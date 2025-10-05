# datan2

## Location
[src/backend/utils/adt/float.c:1836-1861](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1836-L1861)

## Overview
The datan2 function computes the inverse tangent of y/x (two-argument arctangent) and returns the result in radians, handling the correct quadrant determination.

## Definition
```c
Datum datan2(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL ATAN2 function for PostgreSQL, computing the two-argument inverse tangent of arg1/arg2. Unlike the single-argument arctangent function, atan2 considers the signs of both arguments to determine the correct quadrant of the result. The function maps all finite inputs to values in the range [-π, π], providing a complete representation of angles. This makes it particularly useful for converting Cartesian coordinates to polar coordinates.

## Parameters / Member Variables
- `arg1`: The y-coordinate (numerator) for the arctangent calculation
- `arg2`: The x-coordinate (denominator) for the arctangent calculation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (called twice)
  - isnan
  - [get_float8_nan](../g/get_float8_nan.md)
  - atan2 (standard C library function)
  - isinf
  - [float_overflow_error](../f/float_overflow_error.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns NaN if either input is NaN, per POSIX specification
- Maps all inputs to the range [-π, π], providing full quadrant information
- More robust than single-argument arctangent for coordinate system conversions
- Handles special cases like atan2(0,0) according to standard mathematical conventions
- Located in src/backend/utils/adt/float.c:1836-1861

## Simplified Source

```c
Datum datan2(PG_FUNCTION_ARGS) {
    float8 arg1 = PG_GETARG_FLOAT8(0);  // y-coordinate
    float8 arg2 = PG_GETARG_FLOAT8(1);  // x-coordinate

    // Handle NaN input per POSIX spec
    if (isnan(arg1) || isnan(arg2))
        PG_RETURN_FLOAT8(get_float8_nan());

    // Compute two-argument arctangent (maps to [-π, π])
    float8 result = atan2(arg1, arg2);

    // Safety check for overflow (unlikely for atan2)
    if (unlikely(isinf(result)))
        float_overflow_error();

    PG_RETURN_FLOAT8(result);
}
```