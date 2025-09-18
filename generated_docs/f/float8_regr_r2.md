# float8_regr_r2

## Location
src/backend/utils/adt/float.c: 3673 - 3705

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
  - check_float8_array
- Called from (representative examples):
  - Used as aggregate final function in PostgreSQL's REGR_R2 aggregate

## Notes and Other Information
- Part of PostgreSQL's statistical regression functions
- Implements the standard coefficient of determination formula
- R² values range from 0 to 1, where 0 indicates no predictive power and 1 indicates perfect prediction
- The horizontal line case (Syy = 0) returns 1.0 because all y-values are identical, meaning perfect prediction
- Requires exactly 6 elements in the input transition array
- Related to correlation coefficient: R² = (correlation coefficient)²