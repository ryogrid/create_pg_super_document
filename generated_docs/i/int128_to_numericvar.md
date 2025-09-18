# int128_to_numericvar

## Location
src/backend/utils/adt/numeric.c: 8311 - 8356

## Overview
Converts a 128-bit signed integer to PostgreSQL's NumericVar format for internal numeric operations.

## Definition


## Detailed Description
This function converts a 128-bit signed integer (int128) into PostgreSQL's internal NumericVar representation. The conversion process involves extracting digits in base NBASE (10000) by repeatedly dividing the absolute value and storing remainders as digits. The function properly handles sign conversion, allocates sufficient space for the maximum possible digits (40), and sets up all NumericVar fields including weight, scale, and digit count.

The algorithm works backwards through the digit array, filling digits from least significant to most significant by successive division operations. This ensures the final digit array represents the number in PostgreSQL's internal numeric format.

## Parameters / Member Variables
- : The 128-bit signed integer value to convert
- : Pointer to NumericVar structure where the result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [alloc_var](../a/alloc_var.md): Allocate memory for NumericVar digits
  - NUMERIC_NEG: Constant for negative sign
  - NUMERIC_POS: Constant for positive sign
  - NBASE: Numeric base constant (10000)
  - DEC_DIGITS: Decimal digits per NumericDigit
  - NumericDigit: Type for storing numeric digits

- Called from (representative examples):
  - NUMERIC_CAN_BE_SHORT: Numeric optimization checking
  - [int64_div_fast_to_numeric](int64_div_fast_to_numeric.md): Fast division result conversion
  - [numeric_poly_serialize](../n/numeric_poly_serialize.md): Serialization of numeric polynomials
  - [int8_avg_serialize](int8_avg_serialize.md): Serialization of int8 averages
  - [numeric_poly_sum](../n/numeric_poly_sum.md): Polynomial sum calculations
  - [numeric_poly_avg](../n/numeric_poly_avg.md): Polynomial average calculations
  - [numeric_poly_stddev_internal](../n/numeric_poly_stddev_internal.md): Standard deviation calculations
  - [sqrt_var](../s/sqrt_var.md): Square root variable operations

## Notes and Other Information
- Allocates space for 40/DEC_DIGITS digits to handle the maximum int128 range
- Sets dscale to 0 since integers have no fractional part
- Handles zero as a special case with ndigits=0 and weight=0
- Uses unsigned arithmetic (uint128) for the conversion process to handle INT128_MIN correctly
- Weight is set to ndigits-1, representing the position of the most significant digit
- The digits pointer is adjusted to point to the start of actual digits in the allocated array