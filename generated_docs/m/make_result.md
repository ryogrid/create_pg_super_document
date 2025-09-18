# make_result

## Location
src/backend/utils/adt/numeric.c: 7907 - 7922

## Overview
A convenient wrapper function that converts a NumericVar to packed Numeric format with exception-throwing behavior for overflow conditions.

## Definition
```c
static Numeric make_result(const NumericVar *var)
```

## Detailed Description
This function serves as a simplified interface to make_result_opt_error, providing the most common usage pattern where overflow conditions should result in exceptions rather than graceful error handling. It automatically passes NULL as the have_error parameter to make_result_opt_error, ensuring that any overflow or formatting errors will cause the function to throw an exception via ereport().

The function handles all the same conversions as make_result_opt_error, including special values (NaN, ±Infinity), leading/trailing zero truncation, and automatic format selection between short and long numeric representations.

## Parameters / Member Variables
- `var`: Pointer to the source NumericVar structure to be converted to packed format (const, read-only)

## Dependencies
- Functions called/Symbols referenced:
  - make_result_opt_error (with NULL as second parameter)
- Called from (representative examples):
  - numeric_in
  - numeric_recv
  - numeric (type conversion)
  - numeric_sign
  - numeric_round
  - numeric_trunc
  - numeric_ceil
  - numeric_floor
  - generate_series_step_numeric
  - numeric_add_opt_error
  - numeric_sub_opt_error
  - numeric_mul_opt_error
  - numeric_div_opt_error
  - numeric_div_trunc
  - numeric_mod_opt_error
  - numeric_inc
  - numeric_gcd
  - numeric_lcm
  - numeric_fac
  - numeric_sqrt
  - numeric_exp
  - numeric_ln
  - numeric_log
  - numeric_power
  - numeric_trim_scale
  - random_numeric
  - int64_to_numeric
  - int64_div_fast_to_numeric
  - float8_numeric
  - float4_numeric
  - numeric_poly_sum
  - numeric_poly_avg
  - numeric_avg
  - numeric_sum
  - numeric_stddev_internal

## Notes and Other Information
- This is a static function, only accessible within the numeric.c file
- Provides exception-throwing behavior: any overflow or formatting errors will result in ereport() calls rather than returning NULL
- Most commonly used conversion function for NumericVar to Numeric transformation in PostgreSQL
- All the complex logic for format selection, zero handling, and special value processing is delegated to make_result_opt_error
- Used extensively throughout the numeric module for final result generation in arithmetic operations, conversions, and aggregate functions
- The function is essentially a one-liner wrapper that simplifies the API for the majority of use cases where error handling through exceptions is desired