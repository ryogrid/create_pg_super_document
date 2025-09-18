# numeric_div_opt_error

## Location
src/backend/utils/adt/numeric.c: 3160 - 3274

## Overview
Internal version of numeric division that provides optional error handling, allowing callers to handle division errors (like division by zero) without raising exceptions.

## Definition


## Detailed Description
This function performs division of two PostgreSQL Numeric values with optional error handling. Unlike the standard  function, this variant allows the caller to handle errors gracefully by setting a flag instead of throwing exceptions. The function handles special numeric values (NaN, positive/negative infinity) according to IEEE standards and PostgreSQL conventions.

Key behaviors:
- Returns NULL and sets  when division by zero occurs (if error handling is enabled)
- Handles special cases: NaN propagation, infinity division rules
- Uses configurable scale selection for the result precision
- Implements IEEE-compliant special value arithmetic

## Parameters / Member Variables
- : The dividend (numerator) - the Numeric value to be divided
- : The divisor (denominator) - the Numeric value to divide by  
- : Optional pointer to bool flag; if provided, set to true on error instead of throwing exception

## Dependencies
- Functions called/Symbols referenced:
  - NUMERIC_IS_SPECIAL, NUMERIC_IS_NAN, NUMERIC_IS_PINF, NUMERIC_IS_NINF
  - [make_result](../m/make_result.md), make_result_opt_error
  - [numeric_sign_internal](numeric_sign_internal.md)
  - [init_var_from_num](../i/init_var_from_num.md), init_var, free_var
  - [select_div_scale](../s/select_div_scale.md), div_var
- Called from (representative examples):
  - [numeric_div](numeric_div.md)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md) (JSON path execution)
  - [timestamp_part_common](../t/timestamp_part_common.md), timestamptz_part_common

## Notes and Other Information
- This is the core implementation function for numeric division operations in PostgreSQL
- Follows IEEE 754 standards for special value handling (NaN, infinity)
- The scale selection algorithm ensures appropriate precision for division results
- Used internally by higher-level division functions and timestamp arithmetic
- Error handling mechanism allows for more robust numeric computations in complex expressions