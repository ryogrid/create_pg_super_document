# float8_regr_r2

## Location
[src/backend/utils/adt/float.c:3673-3705](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3673-L3705)

## Overview
Computes the coefficient of determination (R-squared) for linear regression from statistical aggregate data stored in a transition array.

## Definition
```c
Datum float8_regr_r2(PG_FUNCTION_ARGS)
```

## Detailed Description
The `float8_regr_r2` function calculates the coefficient of determination (R²) from statistical aggregate data. It takes a 6-element float8 array containing pre-computed statistical values and returns R² using the formula: R² = (Sxy²) / (Sxx * Syy), which represents the square of the correlation coefficient.

R² indicates the proportion of variance in the dependent variable that is predictable from the independent variable. The function handles several edge cases according to SQL specification:
- Returns NULL if N < 1 (no data points)
- Returns NULL if Sxx = 0 (vertical line - undefined regression)
- Returns 1.0 if Syy = 0 (horizontal line - perfect fit)

This function is typically used as a final function in PostgreSQL's aggregate system for computing regression statistics.

## Parameters / Member Variables
- `transarray`: ArrayType pointer containing the 6-element transition array with statistical aggregates:
  - Element 0: N (count of data points)
  - Element 1: sum_x (not used directly)
  - Element 2: Sxx (sum of squares for x)
  - Element 3: sum_y (not used directly)
  - Element 4: Syy (sum of squares for y)
  - Element 5: Sxy (sum of cross-products)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P
  - [check_float8_array](../c/check_float8_array.md)
- Called from (representative examples):
  - Used as aggregate final function in PostgreSQL's REGR_R2 aggregate

## Notes and Other Information
- Part of PostgreSQL's statistical regression functions
- Implements the standard coefficient of determination formula
- R² values range from 0 to 1, where 0 indicates no predictive power and 1 indicates perfect prediction
- The horizontal line case (Syy = 0) returns 1.0 because all y-values are identical, meaning perfect prediction
- Requires exactly 6 elements in the input transition array
- Related to correlation coefficient: R² = (correlation coefficient)²

## Simplified Source

```c
Datum float8_regr_r2(PG_FUNCTION_ARGS) {
    ArrayType *transarray = PG_GETARG_ARRAYTYPE_P(0);

    // Extract regression values from 6-element array
    float8 *transvalues = check_float8_array(transarray, "float8_regr_r2", 6);
    float8 N = transvalues[0];   // Count of data points
    float8 Sxx = transvalues[2]; // Sum of squares for X
    float8 Syy = transvalues[4]; // Sum of squares for Y
    float8 Sxy = transvalues[5]; // Sum of cross-products

    // Return NULL if no data points
    if (N < 1.0)
        PG_RETURN_NULL();

    // Return NULL for vertical line (undefined regression)
    if (Sxx == 0)
        PG_RETURN_NULL();

    // Return 1.0 for horizontal line (perfect fit)
    if (Syy == 0)
        PG_RETURN_FLOAT8(1.0);

    // Return R-squared: (Sxy²) / (Sxx * Syy)
    PG_RETURN_FLOAT8((Sxy * Sxy) / (Sxx * Syy));
}
```