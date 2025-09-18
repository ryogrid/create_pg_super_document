# numeric_stddev_internal

## Location
src/backend/utils/adt/numeric.c: 6222 - 6305

## Overview
Internal workhorse routine that computes standard deviation and variance statistics from accumulated numeric aggregate state.

## Definition
```c
static Numeric numeric_stddev_internal(NumericAggState *state, bool variance, bool sample, bool *is_null)
```

## Detailed Description
This is the core implementation function for calculating both standard deviation and variance aggregates in PostgreSQL. It supports both population and sample statistics calculation. The function implements the mathematical formula for variance: Var = (N * sumX2 - sumX^2) / (N * (N-1)) for sample variance or (N * sumX2 - sumX^2) / (N * N) for population variance.

The function performs the following operations:
1. Validates input state and handles edge cases (NULL, insufficient data)
2. Handles special numeric values (NaN, infinity)
3. Extracts accumulated sums and counts from the state
4. Computes variance using the computational formula
5. Optionally computes standard deviation by taking the square root
6. Handles numerical precision and roundoff errors

## Parameters / Member Variables
- `state`: Pointer to NumericAggState containing accumulated values (sumX, sumX2, N)
- `variance`: If true, returns variance; if false, returns standard deviation
- `sample`: If true, computes sample statistic; if false, computes population statistic
- `is_null`: Output parameter set to true if result should be NULL

## Dependencies
- Functions called/Symbols referenced:
  - NumericAggState (aggregate state structure)
  - NA_TOTAL_COUNT (macro to get total count from state)
  - make_result (creates Numeric from NumericVar)
  - init_var (initializes NumericVar)
  - int64_to_numericvar (converts int64 to NumericVar)
  - accum_sum_final (finalizes accumulated sum)
  - sub_var (subtraction of NumericVar)
  - mul_var (multiplication of NumericVar)
  - cmp_var (comparison of NumericVar)
  - select_div_scale (selects appropriate scale for division)
  - div_var (division of NumericVar)
  - sqrt_var (square root of NumericVar)
  - free_var (frees NumericVar memory)
- Called from (representative examples):
  - numeric_var_samp
  - numeric_stddev_samp
  - numeric_var_pop
  - numeric_stddev_pop
  - numeric_poly_stddev_internal

## Notes and Other Information
- Returns NULL for sample statistics when N <= 1 (mathematically undefined)
- Returns NULL for population statistics when N == 0 (mathematically undefined)
- Returns NaN when any input contains NaN or infinity values
- Uses the computational formula to avoid numerical instability with large sums
- Handles potential roundoff errors that could produce negative variance results
- The function is static (internal to the numeric.c file)
- Supports both sample and population calculations through the sample parameter
- Precision is carefully managed throughout the calculation to maintain accuracy