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
  - [get_float8_nan](../g/get_float8_nan.md) (get NaN value)
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

## Simplified Source

```c
Datum float8_regr_accum(PG_FUNCTION_ARGS) {
    // Get transition array and new Y,X values (Y is first per SQL standard)
    ArrayType *transarray = PG_GETARG_ARRAYTYPE_P(0);
    float8 newvalY = PG_GETARG_FLOAT8(1);
    float8 newvalX = PG_GETARG_FLOAT8(2);

    // Validate and extract 6-element state [N, Sx, Sxx, Sy, Syy, Sxy]
    float8 *transvalues = check_float8_array(transarray, "float8_regr_accum", 6);
    float8 N = transvalues[0];    // Count
    float8 Sx = transvalues[1];   // Sum of X
    float8 Sxx = transvalues[2];  // Sum of squared deviations of X
    float8 Sy = transvalues[3];   // Sum of Y
    float8 Syy = transvalues[4];  // Sum of squared deviations of Y
    float8 Sxy = transvalues[5];  // Sum of cross products

    // Update using Youngs-Cramer algorithm
    N += 1.0;
    Sx += newvalX;
    Sy += newvalY;

    if (transvalues[0] > 0.0) {
        // Update variance and covariance terms for N > 1
        float8 tmpX = newvalX * N - Sx;
        float8 tmpY = newvalY * N - Sy;
        float8 scale = 1.0 / (N * transvalues[0]);

        Sxx += tmpX * tmpX * scale;
        Syy += tmpY * tmpY * scale;
        Sxy += tmpX * tmpY * scale;

        // Handle overflow: finite inputs producing infinite results
        if (isinf(Sx) || isinf(Sxx) || isinf(Sy) || isinf(Syy) || isinf(Sxy)) {
            // Check if overflow is from finite inputs
            bool overflow_from_finite =
                ((isinf(Sx) || isinf(Sxx)) && !isinf(transvalues[1]) && !isinf(newvalX)) ||
                ((isinf(Sy) || isinf(Syy)) && !isinf(transvalues[3]) && !isinf(newvalY)) ||
                (isinf(Sxy) && !isinf(transvalues[1]) && !isinf(newvalX) &&
                               !isinf(transvalues[3]) && !isinf(newvalY));

            if (overflow_from_finite) {
                float_overflow_error();
            }

            // Set NaN for invalid variance/covariance
            if (isinf(Sxx)) Sxx = get_float8_nan();
            if (isinf(Syy)) Syy = get_float8_nan();
            if (isinf(Sxy)) Sxy = get_float8_nan();
        }
    } else {
        // First input: handle special cases (NaN/Inf)
        if (isnan(newvalX) || isinf(newvalX)) {
            Sxx = Sxy = get_float8_nan();
        }
        if (isnan(newvalY) || isinf(newvalY)) {
            Syy = Sxy = get_float8_nan();
        }
    }

    // Return result (optimize by modifying in-place if in aggregate context)
    if (AggCheckCallContext(fcinfo, NULL)) {
        transvalues[0] = N; transvalues[1] = Sx; transvalues[2] = Sxx;
        transvalues[3] = Sy; transvalues[4] = Syy; transvalues[5] = Sxy;
        PG_RETURN_ARRAYTYPE_P(transarray);
    } else {
        Datum transdatums[6] = {
            Float8GetDatumFast(N), Float8GetDatumFast(Sx), Float8GetDatumFast(Sxx),
            Float8GetDatumFast(Sy), Float8GetDatumFast(Syy), Float8GetDatumFast(Sxy)
        };
        ArrayType *result = construct_array(transdatums, 6, FLOAT8OID,
                                          sizeof(float8), FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);
        PG_RETURN_ARRAYTYPE_P(result);
    }
}
```