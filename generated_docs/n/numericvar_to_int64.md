# numericvar_to_int64

## Location
src/backend/utils/adt/numeric.c: 8045 - 8119

## Overview
The `numericvar_to_int64` function converts a PostgreSQL numeric variable to a 64-bit signed integer, performing rounding to the nearest integer and checking for overflow conditions.

## Definition


## Detailed Description
This function performs a safe conversion from PostgreSQL's arbitrary-precision numeric representation to a 64-bit signed integer:

1. **Rounding**: Creates a copy of the input and rounds it to the nearest integer (scale 0)
2. **Zero Handling**: Special case optimization for zero values
3. **Digit Processing**: Processes the numeric representation digit by digit, respecting the weight (position) of each digit
4. **Overflow Detection**: Uses PostgreSQL's overflow-safe arithmetic functions to detect when the result would exceed int64 range
5. **Sign Handling**: Accumulates the value as negative to correctly handle INT64_MIN edge case
6. **Memory Management**: Properly cleans up temporary variables

The algorithm processes digits from most significant to least significant, using the numeric's weight to determine digit positions.

## Parameters / Member Variables
- `var`: Pointer to the source NumericVar structure containing the value to convert
- `result`: Pointer to int64 where the converted value will be stored

## Dependencies
- Functions called/Symbols referenced:
  - `init_var`: Initialize temporary numeric variable
  - [set_var_from_var](../s/set_var_from_var.md): Copy numeric variable content  
  - [round_var](../r/round_var.md): Round to specified decimal places
  - [strip_var](../s/strip_var.md): Remove leading/trailing zeros
  - [free_var](../f/free_var.md): Free numeric variable memory
  - [pg_mul_s64_overflow](../p/pg_mul_s64_overflow.md): Overflow-safe 64-bit multiplication
  - [pg_sub_s64_overflow](../p/pg_sub_s64_overflow.md): Overflow-safe 64-bit subtraction
  - `NBASE`: Numeric digit base constant
  - `NUMERIC_NEG`: Constant for negative sign
  - `PG_INT64_MIN`: Minimum int64 value constant
  - `NumericDigit`: Type for individual digits

- Called from (representative examples):
  - `NUMERIC_CAN_BE_SHORT`: Short numeric validation
  - [numericvar_to_int32](numericvar_to_int32.md): 32-bit integer conversion
  - [numeric_int8_opt_error](numeric_int8_opt_error.md): int8 conversion with error handling
  - [numeric_int2](numeric_int2.md): int2 conversion
  - [power_var](../p/power_var.md): Exponentiation operations

## Notes and Other Information
- Returns `true` on successful conversion, `false` on overflow (no exceptions thrown)
- Handles the INT64_MIN edge case by accumulating values as negative numbers
- Uses overflow-safe arithmetic to prevent undefined behavior during conversion
- Strips leading zeros before processing to optimize performance
- The weight field determines how many digits appear before the decimal point
- Properly manages memory for the temporary rounded variable
- Supports conversion of very large numeric values that fit within int64 range