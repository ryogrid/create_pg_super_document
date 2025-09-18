# numericvar_to_double_no_overflow

## Location
[src/backend/utils/adt/numeric.c:8357-8388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L8357-L8388)

## Overview
Converts a PostgreSQL NumericVar to a double-precision floating-point number, returning +/- HUGE_VAL instead of raising overflow errors.

## Definition


## Detailed Description
This function converts a PostgreSQL NumericVar structure to a double-precision floating-point value (double). Unlike standard conversion functions that may raise overflow errors, this function gracefully handles out-of-range values by returning +/- HUGE_VAL when the numeric value exceeds the representable range of double precision numbers. The conversion is performed by first converting the NumericVar to its string representation, then using the standard C library strtod() function for the actual conversion. The function explicitly ignores ERANGE errors from strtod that would normally indicate overflow conditions.

## Parameters / Member Variables
- : Pointer to the input NumericVar structure containing the numeric value to convert

## Dependencies
- Functions called/Symbols referenced:
  - [get_str_from_var](../g/get_str_from_var.md): Convert NumericVar to string representation
  - strtod: Standard C library function for string to double conversion
  - ereport: PostgreSQL error reporting function
  - [errcode](../e/errcode.md): Error code specification function
  - [errmsg](../e/errmsg.md): Error message formatting function
  - [pfree](../p/pfree.md): PostgreSQL memory deallocation function
  - ERRCODE_INVALID_TEXT_REPRESENTATION: Error code constant
  - HUGE_VAL: IEEE floating-point infinity representation

- Called from (representative examples):
  - NUMERIC_CAN_BE_SHORT: Numeric optimization checking
  - [numeric_exp](numeric_exp.md): Exponential function implementation
  - [numeric_float8_no_overflow](numeric_float8_no_overflow.md): Public interface for overflow-safe conversion
  - [exp_var](../e/exp_var.md): Variable exponential calculations
  - [power_var](../p/power_var.md): Power function calculations

## Notes and Other Information
- Returns +/- HUGE_VAL for out-of-range values instead of raising overflow errors
- Uses string conversion as an intermediate step for maximum compatibility
- Includes validation to ensure complete string parsing (checks for remaining characters)
- Properly manages memory by freeing the temporary string representation
- Designed for use in mathematical functions where overflow should be handled gracefully
- The 'no_overflow' in the name refers to error handling behavior, not prevention of mathematical overflow