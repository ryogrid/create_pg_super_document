# float8_regr_sxy

## Location
src/backend/utils/adt/float.c: 3547 - 3567

## Overview
Returns the sum of products (Sxy) component from a regression transition array, used in statistical correlation and regression calculations.

## Definition


## Detailed Description
This function is part of PostgreSQL's statistical aggregate functions infrastructure, specifically designed to extract the Sxy (sum of products of deviations) value from a 6-element transition array used in regression calculations. The function validates the input array, extracts the count (N) and Sxy values, and returns the Sxy component if the sample size is valid. The Sxy value represents the sum of products of deviations from means: Σ((x_i - x̄)(y_i - ȳ)), which is fundamental for computing correlation coefficients and regression statistics.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to the function's arguments through the function call context
  -  (ArrayType*): A 6-element float8 array containing regression transition values [N, Sx, Sxx, Sy, Syy, Sxy]

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P: Macro to extract ArrayType pointer from function arguments
  - [check_float8_array](../c/check_float8_array.md): Validates and extracts float8 values from the transition array
  - PG_RETURN_NULL: Macro to return NULL when sample size is insufficient
  - PG_RETURN_FLOAT8: Macro to return a float8 value as PostgreSQL Datum
- Called from (representative examples):
  - No direct references found (typically called through SQL aggregate function infrastructure)

## Notes and Other Information
- Returns NULL when N < 1.0 (insufficient sample size)
- Unlike some statistical functions, negative Sxy values are valid and expected
- The function expects exactly 6 elements in the transition array
- Part of PostgreSQL's regression and correlation aggregate functions
- The transition array structure: [0]=N, [1]=Sx, [2]=Sxx, [3]=Sy, [4]=Syy, [5]=Sxy
- Location: src/backend/utils/adt/float.c:3547-3567