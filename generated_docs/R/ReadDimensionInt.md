# ReadDimensionInt

## Location
src/backend/utils/adt/arrayfuncs.c: 519 - 578

## Overview
Parses an integer from a string for array dimension specifications, handling range validation and error conditions during dimension parsing.

## Definition


## Detailed Description
ReadDimensionInt is a static utility function that parses integer values from strings specifically for array dimension processing. It extracts signed integers that represent array bounds (lower and upper limits) while performing careful range validation.

The function uses strtol() for parsing and validates that the result fits within PostgreSQL's integer range (PG_INT32_MIN to PG_INT32_MAX). It handles edge cases gracefully:
- If the input doesn't start with a digit, '-', or '+', it returns success with result = 0
- Leading whitespace is not accepted (caller should handle whitespace)
- The source pointer is advanced past the parsed digits
- ERANGE errors from strtol() are caught and converted to PostgreSQL errors

This function is specifically designed for dimension parsing where integers must be within int32 range to prevent overflow in array size calculations.

## Parameters / Member Variables
- : Pointer to current position in input string, advanced past parsed integer
- : Output parameter for the parsed integer value
- : Original input string (used only for error messages)
- : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - strtol (C standard library)
  - ereturn (PostgreSQL error handling macro)
  - PG_INT32_MIN
  - PG_INT32_MAX
- Called from (representative examples):
  - [ReadArrayDimensions](ReadArrayDimensions.md) (called twice per dimension specification)

## Notes and Other Information
- Static function internal to arrayfuncs.c
- Does not skip leading whitespace - caller must handle whitespace
- Returns true even when no digits are found (leaves srcptr unchanged)
- Performs strict range checking to prevent integer overflow in downstream calculations
- Uses errno to detect strtol() overflow conditions
- Designed specifically for array dimension parsing where bounds must fit in int32