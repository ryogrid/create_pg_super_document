# float8_regr_syy

## Location
[src/backend/utils/adt/float.c:3526-3546](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3526-L3546)

## Overview
Extracts and returns the sum of squared deviations of Y values (Syy) from a regression transition state, implementing the SQL REGR_SYY aggregate function.

## Definition
Datum float8_regr_syy(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as a final function for the SQL REGR_SYY aggregate, which computes the sum of squares of deviations of the dependent variable Y from its mean. It extracts the Syy value (stored at index 4) from the 6-element regression transition array that was built up during the accumulation phase. The function returns NULL when there are no data points (N < 1) since the sum of squared deviations is undefined for empty datasets. The Syy value represents Σ(Y - Ȳ)² where Ȳ is the mean of Y values.

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
- The Syy value is guaranteed to be non-negative due to its mathematical definition as sum of squares
- Part of PostgreSQL's implementation of SQL:2003 regression statistical functions
- Used in conjunction with float8_regr_accum for the complete REGR_SYY aggregate implementation
- The transition array index 4 specifically contains the sum of squared deviations of Y values
- Complementary to float8_regr_sxx which handles the X variable squared deviations

## Simplified Source

```c
Datum
float8_regr_syy(PG_FUNCTION_ARGS)
{
    ArrayType *transarray = PG_GETARG_ARRAYTYPE_P(0);

    // Extract N (count) and Syy (sum of squared deviations of Y) from 6-element regression state
    float8 *transvalues = check_float8_array(transarray, "float8_regr_syy", 6);
    float8 N = transvalues[0];    // Count of data points
    float8 Syy = transvalues[4];  // Sum of squared deviations of Y: Σ(Y - Ȳ)²

    // Return NULL for empty datasets
    if (N < 1.0)
        PG_RETURN_NULL();

    // Return sum of squared deviations of Y (guaranteed non-negative)
    PG_RETURN_FLOAT8(Syy);
}
```