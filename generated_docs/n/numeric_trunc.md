# numeric_trunc

## Location
src/backend/utils/adt/numeric.c: 1595 - 1644

## Overview
The numeric_trunc function implements PostgreSQL's TRUNC() SQL function for NUMERIC data types, truncating a value to a specified number of digits after the decimal point without rounding.

## Definition


## Detailed Description
This function provides the SQL-accessible interface for truncating NUMERIC values in PostgreSQL. Unlike rounding, truncation simply removes digits beyond the specified scale without any rounding behavior. The function accepts two arguments: the numeric value to truncate and the scale (number of digits after the decimal point). Like numeric_round, it supports negative scale values, which truncates digits before the decimal point, following Oracle's interpretation.

The implementation is structurally similar to numeric_round but uses trunc_var() instead of round_var() for the core operation. It handles special values (NaN and infinities) by returning duplicates, includes comprehensive bounds checking on the scale parameter, and uses PostgreSQL's internal NumericVar representation for precise decimal arithmetic.

Key differences from rounding:
- Uses trunc_var() which simply discards digits rather than rounding
- No rounding logic - digits are simply removed
- Same bounds checking and special value handling as numeric_round

## Parameters / Member Variables
- Input argument 0: The NUMERIC value to be truncated
- Input argument 1: The scale (int32) - number of digits after decimal point (can be negative)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC: Extracts the NUMERIC argument from function arguments
  - PG_GETARG_INT32: Extracts the scale parameter
  - NUMERIC_IS_SPECIAL: Checks if the numeric value is special (NaN/infinity)
  - duplicate_numeric: Creates a copy of special values
  - init_var: Initializes a NumericVar structure
  - set_var_from_num: Converts Numeric to NumericVar
  - trunc_var: Performs the actual truncation operation
  - make_result: Converts NumericVar back to Numeric
  - free_var: Frees NumericVar memory
  - PG_RETURN_NUMERIC: Returns the result as a NUMERIC Datum
  - NUMERIC_WEIGHT_MAX, NUMERIC_DSCALE_MAX, DEC_DIGITS: Constants for bounds checking

- Called from (representative examples):
  - getArrayIndex: Used in JSON path execution for array index calculations

## Notes and Other Information
- The function is located in src/backend/utils/adt/numeric.c at lines 1595-1644
- This is the public SQL interface for the TRUNC() function for NUMERIC types
- Supports Oracle-compatible negative scale truncation (truncates digits before decimal point)
- The key difference from numeric_round is the use of trunc_var() instead of round_var()
- Includes the same sophisticated bounds checking as numeric_round to prevent overflow
- For negative scale results, the output dscale is set to 0 to prevent negative display scale
- Uses PostgreSQL's high-precision NumericVar arithmetic for accurate decimal truncation
- Memory management includes proper initialization and cleanup of temporary variables
- Less commonly used than numeric_round, with fewer internal references in the codebase