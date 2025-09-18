# numeric_sign

## Location
src/backend/utils/adt/numeric.c: 1508 - 1540

## Overview
The numeric_sign function implements PostgreSQL's SIGN() SQL function for NUMERIC data types, returning -1, 0, or 1 based on whether the input is negative, zero, or positive respectively.

## Definition


## Detailed Description
This function provides the SQL-accessible interface for determining the sign of NUMERIC values in PostgreSQL. It handles all special cases including NaN (Not a Number) and infinities, returning appropriate NUMERIC results rather than simple integer values. The function serves as a wrapper around the internal numeric_sign_internal function, converting its integer result into proper NUMERIC return values.

For NaN inputs, the function returns NaN. For all other values (including infinities), it delegates to numeric_sign_internal and converts the resulting integer (-1, 0, 1) into the corresponding NUMERIC constants (const_minus_one, const_zero, const_one) using make_result.

## Parameters / Member Variables
- Input argument 0: The NUMERIC value whose sign is to be determined

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC: Extracts the NUMERIC argument from function arguments
  - NUMERIC_IS_NAN: Checks if the numeric value is NaN
  - make_result: Converts internal numeric representation to NUMERIC datum
  - PG_RETURN_NUMERIC: Returns the result as a NUMERIC Datum
  - numeric_sign_internal: Internal function that performs the actual sign determination
  - const_nan, const_zero, const_one, const_minus_one: Pre-defined NUMERIC constants

- Called from (representative examples):
  - This function is directly callable from SQL as the SIGN() function
  - No other internal PostgreSQL functions currently reference this symbol

## Notes and Other Information
- The function is located in src/backend/utils/adt/numeric.c at lines 1508-1540
- This is the public SQL interface for the SIGN() function for NUMERIC types
- Properly handles NaN by returning NaN, unlike the internal version which assumes NaN is pre-handled
- Uses pre-defined constants for common values (-1, 0, 1) for efficiency
- The switch statement includes an assertion that should never be reached, indicating complete coverage of possible return values from numeric_sign_internal
- Returns NUMERIC values rather than integers, maintaining type consistency within PostgreSQL's SQL system