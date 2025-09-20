# float8_corr

## Location
[src/backend/utils/adt/float.c:3644-3672](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3644-L3672)

## Overview
Computes the correlation coefficient between two sets of values from statistical aggregate data stored in a transition array.

## Definition

```c
Datum
float8_corr(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function calculates the Pearson correlation coefficient from statistical aggregate data. It takes a 6-element float8 array containing pre-computed statistical values (N, sum_x, sum_x2, sum_y, sum_y2, sum_xy) and returns the correlation coefficient using the formula: r = Sxy / sqrt(Sxx * Syy), where Sxx and Syy are the sum of squares and Sxy is the sum of cross-products.

The function handles several edge cases:
- Returns NULL if N < 1 (no data points)
- Returns NULL if either Sxx or Syy equals 0 (horizontal or vertical lines have undefined correlation)

This function is typically used as a final function in PostgreSQL's aggregate system for computing correlations.

## Parameters / Member Variables
- : ArrayType pointer containing the 6-element transition array with statistical aggregates:
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
  - Used as aggregate final function in PostgreSQL's correlation aggregate

## Notes and Other Information
- Part of PostgreSQL's statistical aggregate functions
- Implements the standard Pearson correlation coefficient formula
- Handles degenerate cases (no variance) by returning NULL per SQL specification
- Requires exactly 6 elements in the input transition array
- The correlation coefficient ranges from -1 to +1, where -1 indicates perfect negative correlation, 0 indicates no correlation, and +1 indicates perfect positive correlation