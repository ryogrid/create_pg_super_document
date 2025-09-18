# float8_covar_samp

## Location
[src/backend/utils/adt/float.c:3625-3643](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3625-L3643)

## Overview
Computes the sample covariance from a regression transition array, measuring the degree to which two variables vary together in a sample dataset.

## Definition
```c
Datum float8_covar_samp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function calculates the sample covariance by extracting the sum of products (Sxy) from a 6-element regression transition array and dividing by the degrees of freedom (N-1). Sample covariance differs from population covariance by using Bessel's correction (dividing by N-1 instead of N), which provides an unbiased estimator when working with sample data rather than the complete population. The covariance measures how much two random variables change together, with positive values indicating they tend to increase together, negative values indicating they move in opposite directions, and zero indicating no linear relationship.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to the function's arguments through the function call context
  - `transarray` (ArrayType*): A 6-element float8 array containing regression transition values [N, Sx, Sxx, Sy, Syy, Sxy]

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P: Macro to extract ArrayType pointer from function arguments
  - [check_float8_array](../c/check_float8_array.md): Validates and extracts float8 values from the transition array
  - PG_RETURN_NULL: Macro to return NULL when sample size is insufficient
  - PG_RETURN_FLOAT8: Macro to return a float8 value as PostgreSQL Datum
- Called from (representative examples):
  - No direct references found (typically called through SQL aggregate function infrastructure)

## Notes and Other Information
- Returns NULL when N < 2.0 (insufficient sample size for variance calculation)
- Computes sample covariance as Sxy/(N-1) using Bessel's correction
- Requires at least 2 data points, unlike population covariance which only requires 1
- Can return positive, negative, or zero values depending on the relationship between variables
- The function expects exactly 6 elements in the transition array
- Part of PostgreSQL's statistical aggregate functions suite
- The transition array structure: [0]=N, [1]=Sx, [2]=Sxx, [3]=Sy, [4]=Syy, [5]=Sxy
- Location: src/backend/utils/adt/float.c:3625-3643