# make_result

## Location
[src/backend/utils/adt/numeric.c:7907-7922](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L7907-L7922)

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
  - [make_result_opt_error](make_result_opt_error.md) (with NULL as second parameter)
- Called from (representative examples):
  - [numeric_in](../n/numeric_in.md)
  - [numeric_recv](../n/numeric_recv.md)
  - [numeric](../n/numeric.md) (type conversion)
  - [numeric_sign](../n/numeric_sign.md)
  - [numeric_round](../n/numeric_round.md)
  - [numeric_trunc](../n/numeric_trunc.md)
  - [numeric_ceil](../n/numeric_ceil.md)
  - [numeric_floor](../n/numeric_floor.md)
  - [generate_series_step_numeric](../g/generate_series_step_numeric.md)
  - [numeric_add_opt_error](../n/numeric_add_opt_error.md)
  - [numeric_sub_opt_error](../n/numeric_sub_opt_error.md)
  - [numeric_mul_opt_error](../n/numeric_mul_opt_error.md)
  - [numeric_div_opt_error](../n/numeric_div_opt_error.md)
  - [numeric_div_trunc](../n/numeric_div_trunc.md)
  - [numeric_mod_opt_error](../n/numeric_mod_opt_error.md)
  - [numeric_inc](../n/numeric_inc.md)
  - [numeric_gcd](../n/numeric_gcd.md)
  - [numeric_lcm](../n/numeric_lcm.md)
  - [numeric_fac](../n/numeric_fac.md)
  - [numeric_sqrt](../n/numeric_sqrt.md)
  - [numeric_exp](../n/numeric_exp.md)
  - [numeric_ln](../n/numeric_ln.md)
  - [numeric_log](../n/numeric_log.md)
  - [numeric_power](../n/numeric_power.md)
  - [numeric_trim_scale](../n/numeric_trim_scale.md)
  - [random_numeric](../r/random_numeric.md)
  - [int64_to_numeric](../i/int64_to_numeric.md)
  - [int64_div_fast_to_numeric](../i/int64_div_fast_to_numeric.md)
  - [float8_numeric](../f/float8_numeric.md)
  - [float4_numeric](../f/float4_numeric.md)
  - [numeric_poly_sum](../n/numeric_poly_sum.md)
  - [numeric_poly_avg](../n/numeric_poly_avg.md)
  - [numeric_avg](../n/numeric_avg.md)
  - [numeric_sum](../n/numeric_sum.md)
  - [numeric_stddev_internal](../n/numeric_stddev_internal.md)

## Notes and Other Information
- This is a static function, only accessible within the numeric.c file
- Provides exception-throwing behavior: any overflow or formatting errors will result in ereport() calls rather than returning NULL
- Most commonly used conversion function for NumericVar to Numeric transformation in PostgreSQL
- All the complex logic for format selection, zero handling, and special value processing is delegated to make_result_opt_error
- Used extensively throughout the numeric module for final result generation in arithmetic operations, conversions, and aggregate functions
- The function is essentially a one-liner wrapper that simplifies the API for the majority of use cases where error handling through exceptions is desired