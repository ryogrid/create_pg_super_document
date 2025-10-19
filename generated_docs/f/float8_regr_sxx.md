# float8_regr_sxx

## Location
[src/backend/utils/adt/float.c:3505-3525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3505-L3525)

## Overview
Extracts and returns the sum of squared deviations of X values (Sxx) from a regression transition state, implementing the SQL REGR_SXX aggregate function.

## Definition
Datum float8_regr_sxx(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as a final function for the SQL REGR_SXX aggregate, which computes the sum of squares of deviations of the independent variable X from its mean. It extracts the Sxx value (stored at index 2) from the 6-element regression transition array that was built up during the accumulation phase. The function returns NULL when there are no data points (N < 1) since the sum of squared deviations is undefined for empty datasets. The Sxx value represents Σ(X - X̄)² where X̄ is the mean of X values.

## Parameters / Member Variables
- transarray: ArrayType pointer containing the 6-element float8 regression transition state [N, Sx, Sxx, Sy, Syy, Sxy]

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P (extract array argument)
  - [check_float8_array](../c/check_float8_array.md) (validate transition array has 6 elements)
- Called from (representative examples):
  - No direct callers found (used through SQL aggregate system)

## Notes and Other Information
- Returns NULL for empty datasets (N < 1) as sum of squared deviations is undefined
- The Sxx value is guaranteed to be non-negative due to its mathematical definition as sum of squares
- Part of PostgreSQL's implementation of SQL:2003 regression statistical functions
- Used in conjunction with float8_regr_accum for the complete REGR_SXX aggregate implementation
- The transition array index 2 specifically contains the sum of squared deviations of X values

## Simplified Source

```c
Datum
float8_regr_sxx(PG_FUNCTION_ARGS)
{
    ArrayType *transarray = PG_GETARG_ARRAYTYPE_P(0);

    // Extract N (count) and Sxx (sum of squared deviations of X) from 6-element regression state
    float8 *transvalues = check_float8_array(transarray, "float8_regr_sxx", 6);
    float8 N = transvalues[0];    // Count of data points
    float8 Sxx = transvalues[2];  // Sum of squared deviations of X: Σ(X - X̄)²

    // Return NULL for empty datasets
    if (N < 1.0)
        PG_RETURN_NULL();

    // Return sum of squared deviations of X (guaranteed non-negative)
    PG_RETURN_FLOAT8(Sxx);
}
```