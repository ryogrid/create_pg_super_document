# float8_regr_intercept

## Location
src/backend/utils/adt/float.c: 3733 - 3776

## Overview
Computes the y-intercept of the linear regression line from statistical aggregate data stored in a transition array.

## Definition
```c
Datum float8_regr_intercept(PG_FUNCTION_ARGS)
```

## Detailed Description
The `float8_regr_intercept` function calculates the y-intercept of the least-squares linear regression line from statistical aggregate data. It takes a 6-element float8 array containing pre-computed statistical values and returns the y-intercept using the formula: intercept = (Sy - Sx * Sxy / Sxx) / N, which represents where the regression line crosses the y-axis (when x = 0).

The y-intercept is the predicted value of the dependent variable when the independent variable equals zero. The function handles edge cases:
- Returns NULL if N < 1 (no data points)
- Returns NULL if Sxx = 0 (vertical line - undefined regression)

This function is typically used as a final function in PostgreSQL's aggregate system for computing regression statistics.

## Parameters / Member Variables
- `transarray`: ArrayType pointer containing the 6-element transition array with statistical aggregates:
  - Element 0: N (count of data points)
  - Element 1: Sx (sum of x values)
  - Element 2: Sxx (sum of squares for x)
  - Element 3: Sy (sum of y values)
  - Element 4: Syy (not used directly)
  - Element 5: Sxy (sum of cross-products)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P
  - [check_float8_array](../c/check_float8_array.md)
- Called from (representative examples):
  - Used as aggregate final function in PostgreSQL's REGR_INTERCEPT aggregate

## Notes and Other Information
- Part of PostgreSQL's statistical regression functions
- Implements the standard least-squares regression intercept formula
- The intercept can be positive, negative, or zero depending on the data
- For a vertical line (all x-values identical), the intercept is undefined and NULL is returned per SQL specification
- Requires exactly 6 elements in the input transition array
- Combined with the slope, the intercept fully defines the linear regression line: y = slope * x + intercept
- Uses more transition array elements than slope calculation (Sx and Sy are needed)