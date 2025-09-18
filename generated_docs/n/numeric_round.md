# numeric_round

## Location
src/backend/utils/adt/numeric.c: 1541 - 1594

## Overview
The numeric_round function implements PostgreSQL's ROUND() SQL function for NUMERIC data types, rounding a value to a specified number of digits after the decimal point.

## Definition


## Detailed Description
This function provides the SQL-accessible interface for rounding NUMERIC values in PostgreSQL. It accepts two arguments: the numeric value to round and the scale (number of digits after the decimal point). The function supports negative scale values, which rounds digits before the decimal point, following Oracle's interpretation of rounding.

The implementation handles special values (NaN and infinities) by returning duplicates, and includes comprehensive bounds checking on the scale parameter to prevent overflow. It uses PostgreSQL's internal NumericVar representation for the actual rounding calculation via round_var(), ensuring precise decimal arithmetic.

Key features include:
- Support for negative scale values (rounding before decimal point)
- Bounds checking to prevent numeric overflow
- Proper handling of special values (NaN, infinity)
- Memory management with proper variable initialization and cleanup

## Parameters / Member Variables
- Input argument 0: The NUMERIC value to be rounded
- Input argument 1: The scale (int32) - number of digits after decimal point (can be negative)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC: Extracts the NUMERIC argument from function arguments
  - PG_GETARG_INT32: Extracts the scale parameter
  - NUMERIC_IS_SPECIAL: Checks if the numeric value is special (NaN/infinity)
  - duplicate_numeric: Creates a copy of special values
  - init_var: Initializes a NumericVar structure
  - set_var_from_num: Converts Numeric to NumericVar
  - round_var: Performs the actual rounding operation
  - make_result: Converts NumericVar back to Numeric
  - free_var: Frees NumericVar memory
  - PG_RETURN_NUMERIC: Returns the result as a NUMERIC Datum
  - NUMERIC_WEIGHT_MAX, NUMERIC_DSCALE_MAX, DEC_DIGITS: Constants for bounds checking

- Called from (representative examples):
  - cash_numeric: Used in money type conversions with rounding
  - numeric_to_char: Used in numeric formatting operations
  - timestamp_part_common: Used in timestamp/timestamptz part extraction
  - timestamptz_part_common: Used in timestamptz part extraction

## Notes and Other Information
- The function is located in src/backend/utils/adt/numeric.c at lines 1541-1594
- This is the public SQL interface for the ROUND() function for NUMERIC types
- Supports Oracle-compatible negative scale rounding (rounds digits before decimal point)
- Includes sophisticated bounds checking: scale is limited to prevent overflow based on maximum numeric weight and scale
- For negative scale results, the output dscale is set to 0 to prevent negative display scale
- Uses PostgreSQL's high-precision NumericVar arithmetic for accurate decimal rounding
- Memory management includes proper initialization and cleanup of temporary variables