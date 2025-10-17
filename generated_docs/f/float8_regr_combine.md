# float8_regr_combine

## Location
[src/backend/utils/adt/float.c:3371-3504](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3371-L3504)

## Overview
Combines two 6-element regression transition state arrays using a generalized Youngs-Cramer algorithm for parallel and two-stage aggregate computation in PostgreSQL.

## Definition
Datum float8_regr_combine(PG_FUNCTION_ARGS)

## Detailed Description
This function implements the combine phase for parallel regression aggregates, merging two independent transition states into a single combined state. It uses a mathematically sound extension of the Youngs-Cramer algorithm to combine the statistical values: N (count), Sx (sum of X), Sxx (sum of squared deviations of X), Sy (sum of Y), Syy (sum of squared deviations of Y), and Sxy (sum of cross products). The algorithm handles the complex mathematics of combining squared deviations and cross products while maintaining numerical stability. Special cases for empty states (N=0) are handled separately to avoid division-by-zero errors.

## Parameters / Member Variables
- transarray1: ArrayType pointer containing the first 6-element float8 transition state [N1, Sx1, Sxx1, Sy1, Syy1, Sxy1]
- transarray2: ArrayType pointer containing the second 6-element float8 transition state [N2, Sx2, Sxx2, Sy2, Syy2, Sxy2]

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P (extract array arguments)
  - [check_float8_array](../c/check_float8_array.md) (validate transition arrays)
  - [float8_pl](float8_pl.md) (safe float8 addition with overflow checking)
  - isinf (check for infinite values)
  - [float_overflow_error](float_overflow_error.md) (report overflow errors)
  - unlikely (compiler optimization hint)
  - [AggCheckCallContext](../A/AggCheckCallContext.md) (check if in aggregate context)
  - Float8GetDatumFast (convert float8 to Datum)
  - [construct_array](../c/construct_array.md) (build new array)
- Called from (representative examples):
  - No direct callers found (used by parallel aggregate system)

## Notes and Other Information
- Specifically designed for two-stage and parallel aggregation scenarios
- Should only be called within aggregate context according to function documentation
- Uses generalized Youngs-Cramer formulas for combining squared deviations and cross products
- Handles edge cases where one or both transition states are empty (N=0)
- Includes comprehensive overflow detection for Sxx, Syy, and Sxy calculations
- Optimizes memory usage by modifying first input array in-place when in aggregate context
- Essential for PostgreSQL's parallel query processing capabilities for statistical functions

## Simplified Source

```c
Datum float8_regr_combine(PG_FUNCTION_ARGS) {
    // Extract two transition state arrays
    ArrayType *transarray1 = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *transarray2 = PG_GETARG_ARRAYTYPE_P(1);

    // Validate and extract 6-element float8 arrays
    float8 *values1 = check_float8_array(transarray1, "float8_regr_combine", 6);
    float8 *values2 = check_float8_array(transarray2, "float8_regr_combine", 6);

    // Extract transition state components
    float8 N1 = values1[0], Sx1 = values1[1], Sxx1 = values1[2];
    float8 Sy1 = values1[3], Syy1 = values1[4], Sxy1 = values1[5];

    float8 N2 = values2[0], Sx2 = values2[1], Sxx2 = values2[2];
    float8 Sy2 = values2[3], Syy2 = values2[4], Sxy2 = values2[5];

    float8 N, Sx, Sxx, Sy, Syy, Sxy;

    // Handle special cases: empty transition states
    if (N1 == 0.0) {
        // Use second state entirely
        N = N2; Sx = Sx2; Sxx = Sxx2;
        Sy = Sy2; Syy = Syy2; Sxy = Sxy2;
    } else if (N2 == 0.0) {
        // Use first state entirely
        N = N1; Sx = Sx1; Sxx = Sxx1;
        Sy = Sy1; Syy = Syy1; Sxy = Sxy1;
    } else {
        // Combine using Youngs-Cramer algorithm
        N = N1 + N2;
        Sx = float8_pl(Sx1, Sx2);

        // Combine squared deviations with correction terms
        float8 mean_diff_x = Sx1 / N1 - Sx2 / N2;
        float8 mean_diff_y = Sy1 / N1 - Sy2 / N2;

        Sxx = Sxx1 + Sxx2 + N1 * N2 * mean_diff_x * mean_diff_x / N;
        if (unlikely(isinf(Sxx)) && !isinf(Sxx1) && !isinf(Sxx2))
            float_overflow_error();

        Sy = float8_pl(Sy1, Sy2);
        Syy = Syy1 + Syy2 + N1 * N2 * mean_diff_y * mean_diff_y / N;
        if (unlikely(isinf(Syy)) && !isinf(Syy1) && !isinf(Syy2))
            float_overflow_error();

        Sxy = Sxy1 + Sxy2 + N1 * N2 * mean_diff_x * mean_diff_y / N;
        if (unlikely(isinf(Sxy)) && !isinf(Sxy1) && !isinf(Sxy2))
            float_overflow_error();
    }

    // Return result: modify in-place if in aggregate context, else create new array
    if (AggCheckCallContext(fcinfo, NULL)) {
        values1[0] = N; values1[1] = Sx; values1[2] = Sxx;
        values1[3] = Sy; values1[4] = Syy; values1[5] = Sxy;
        PG_RETURN_ARRAYTYPE_P(transarray1);
    } else {
        Datum datums[6] = {
            Float8GetDatumFast(N), Float8GetDatumFast(Sx), Float8GetDatumFast(Sxx),
            Float8GetDatumFast(Sy), Float8GetDatumFast(Syy), Float8GetDatumFast(Sxy)
        };
        ArrayType *result = construct_array(datums, 6, FLOAT8OID,
            sizeof(float8), FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);
        PG_RETURN_ARRAYTYPE_P(result);
    }
}
```