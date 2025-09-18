# float8_stddev_samp

## Location
[src/backend/utils/adt/float.c:3204-3246](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3204-L3246)

## Overview
Calculates the sample standard deviation for a float8 aggregate transition state, implementing the SQL standard STDDEV_SAMP function for double precision floating-point values.

## Definition


## Detailed Description
This function computes the sample standard deviation from aggregate transition values stored in a float8 array. The sample standard deviation uses Bessel's correction (dividing by N-1 instead of N) to provide an unbiased estimator of population standard deviation. The function extracts the count (N) and sum of squared deviations (Sxx) from the transition array, then applies the formula: sqrt(Sxx / (N - 1)). Returns NULL when there are insufficient data points (N ≤ 1) since sample standard deviation is undefined for single values or empty sets.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - : ArrayType pointer containing the float8 transition state [N, Sx, Sxx] where N is count, Sx is sum, and Sxx is sum of squared deviations

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P (macro for extracting array argument)
  - [check_float8_array](../c/check_float8_array.md) (validates and extracts float8 values from array)
- Called from (representative examples):
  - No direct callers found (likely used through SQL aggregate system)

## Notes and Other Information
- Returns NULL for N ≤ 1 since sample standard deviation requires at least 2 data points
- Uses sqrt() function from math library for final calculation
- Part of PostgreSQL's statistical aggregate function suite
- Implements SQL:2003 standard STDDEV_SAMP aggregate function
- The Sxx value is guaranteed to be non-negative due to its mathematical definition