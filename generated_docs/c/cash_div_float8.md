# cash_div_float8

## Location
src/backend/utils/adt/cash.c: 130 - 142

## Overview
A private inline function that performs safe division of a Cash value by a float8 (double precision floating point) value with range checking and proper rounding.

## Definition


## Detailed Description
The  function divides a Cash value (64-bit signed integer representing monetary amounts) by a float8 value (double precision floating point number). The function performs the division in floating point arithmetic using PostgreSQL's safe  function, applies proper rounding using , and then checks that the result is valid (not NaN and fits within the range of a 64-bit signed integer). If the result is out of range, invalid, or if division by zero occurs, it reports a "money out of range" error. This ensures that monetary division operations maintain precision and stay within valid bounds.

## Parameters / Member Variables
- : Cash value to be divided (dividend)
- : float8 (double precision) divisor value

## Dependencies
- Functions called/Symbols referenced:
  - Cash (type)
  - float8 (type)
  - rint (rounding function)
  - float8_div (PostgreSQL's safe float8 division)
  - isnan (NaN detection function)
  - FLOAT8_FITS_IN_INT64 (range checking macro)
  - ereport (error reporting)
  - errcode (error code specification)
  - errmsg (error message formatting)
  - ERROR (error level constant)
  - ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE (specific error code)
- Called from (representative examples):
  - cash_div_flt8
  - cash_div_flt4

## Notes and Other Information
- Declared as  for performance optimization in arithmetic operations
- Uses  for proper rounding to nearest integer, maintaining monetary precision
- Performs floating point division first, then converts back to integer Cash type
- Checks for both NaN results (including division by zero) and integer overflow conditions
- Part of PostgreSQL's cash data type implementation for mixed-type arithmetic operations
- Enables division of monetary values by scaling factors or percentage denominators
- The intermediate float8 calculation allows for fractional division while maintaining final integer precision
- Complementary function to  for division operations