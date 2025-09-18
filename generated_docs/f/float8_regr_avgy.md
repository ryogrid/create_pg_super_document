# float8_regr_avgy

## Location
[src/backend/utils/adt/float.c:3587-3605](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3587-L3605)

## Overview
Computes and returns the average of Y values from a regression transition array, used in statistical regression analysis.

## Definition
```c
Datum float8_regr_avgy(PG_FUNCTION_ARGS)
```

## Detailed Description
This function extracts the average Y value from a 6-element regression transition array. It validates the input array, retrieves the count (N) and sum of Y values (Sy), then computes and returns the arithmetic mean (Sy/N). This is a fundamental component in regression analysis, providing the mean of the dependent variable which is used in various regression statistics calculations such as correlation coefficients, regression slopes, and intercepts.

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
- Returns NULL when N < 1.0 (no data points available)
- Computes Sy/N where Sy is the sum of Y values and N is the count
- The function expects exactly 6 elements in the transition array
- Part of PostgreSQL's regression aggregate functions suite
- The transition array structure: [0]=N, [1]=Sx, [2]=Sxx, [3]=Sy, [4]=Syy, [5]=Sxy
- Location: src/backend/utils/adt/float.c:3587-3605