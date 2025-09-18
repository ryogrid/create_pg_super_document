# float8_regr_accum

## Location
[src/backend/utils/adt/float.c:3247-3370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3247-L3370)

## Overview
Accumulates transition state values for SQL regression aggregate functions using the Youngs-Cramer algorithm to maintain numerical stability and reduce rounding errors.

## Definition
Datum float8_regr_accum(PG_FUNCTION_ARGS)

## Detailed Description
This function implements the accumulation phase for SQL binary regression aggregates (such as REGR_SLOPE, REGR_INTERCEPT, CORR, etc.). It maintains a 6-element transition state array containing statistical values: N (count), Sx (sum of X), Sxx (sum of squared deviations of X), Sy (sum of Y), Syy (sum of squared deviations of Y), and Sxy (sum of cross products). The function uses the numerically stable Youngs-Cramer algorithm to incrementally update these values when a new (Y,X) data point is added. It includes comprehensive overflow detection and NaN handling to ensure robust statistical calculations.

## Parameters / Member Variables
- transarray: ArrayType pointer containing the current 6-element float8 transition state [N, Sx, Sxx, Sy, Syy, Sxy]
- newvalY: float8 value representing the Y coordinate of the new data point (first SQL argument)
- newvalX: float8 value representing the X coordinate of the new data point (second SQL argument)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P (extract array argument)
  - PG_GETARG_FLOAT8 (extract float8 arguments)
  - [check_float8_array](../c/check_float8_array.md) (validate transition array)
  - isinf (check for infinite values)
  - isnan (check for NaN values)
  - [float_overflow_error](float_overflow_error.md) (report overflow errors)
  - get_float8_nan (get NaN value)
  - [AggCheckCallContext](../A/AggCheckCallContext.md) (check if in aggregate context)
  - Float8GetDatumFast (convert float8 to Datum)
  - [construct_array](../c/construct_array.md) (build new array)
- Called from (representative examples):
  - No direct callers found (used through SQL aggregate system)

## Notes and Other Information
- Uses Youngs-Cramer algorithm for numerical stability in incremental statistical calculations
- Note that Y is the first argument to regression aggregates, following SQL standard conventions
- Handles special cases for infinite and NaN inputs to prevent incorrect variance calculations
- Optimizes memory usage by modifying the input array in-place when called in aggregate context
- Part of PostgreSQL's implementation of SQL:2003 binary aggregate functions
- The 6-element transition state supports multiple regression statistics without recalculation