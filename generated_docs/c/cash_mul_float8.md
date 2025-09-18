# cash_mul_float8

## Location
[src/backend/utils/adt/cash.c:117-129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L117-L129)

## Overview
A private inline function that performs safe multiplication of a Cash value by a float8 (double precision floating point) value with range checking and proper rounding.

## Definition


## Detailed Description
The  function multiplies a Cash value (64-bit signed integer representing monetary amounts) by a float8 value (double precision floating point number). The function performs the multiplication in floating point arithmetic, applies proper rounding using , and then checks that the result is valid (not NaN and fits within the range of a 64-bit signed integer). If the result is out of range or invalid, it reports a "money out of range" error. This ensures that monetary multiplication operations maintain precision and stay within valid bounds.

## Parameters / Member Variables
- : Cash value to be multiplied
- : float8 (double precision) multiplier value

## Dependencies
- Functions called/Symbols referenced:
  - Cash (type)
  - float8 (type)
  - rint (rounding function)
  - [float8_mul](../f/float8_mul.md) (PostgreSQL's safe float8 multiplication)
  - isnan (NaN detection function)
  - FLOAT8_FITS_IN_INT64 (range checking macro)
  - ereport (error reporting)
  - [errcode](../e/errcode.md) (error code specification)
  - [errmsg](../e/errmsg.md) (error message formatting)
  - ERROR (error level constant)
  - ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE (specific error code)
- Called from (representative examples):
  - [cash_mul_flt8](cash_mul_flt8.md)
  - [flt8_mul_cash](../f/flt8_mul_cash.md)
  - [cash_mul_flt4](cash_mul_flt4.md)
  - [flt4_mul_cash](../f/flt4_mul_cash.md)

## Notes and Other Information
- Declared as  for performance optimization in arithmetic operations
- Uses  for proper rounding to nearest integer, maintaining monetary precision
- Performs floating point multiplication first, then converts back to integer Cash type
- Checks for both NaN results and integer overflow conditions
- Part of PostgreSQL's cash data type implementation for mixed-type arithmetic operations
- Enables multiplication of monetary values by percentage factors or scaling values
- The intermediate float8 calculation allows for fractional multiplication while maintaining final integer precision