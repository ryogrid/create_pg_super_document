# dump_numeric

## Location
src/backend/utils/adt/numeric.c: 6874 - 6915

## Overview
A debugging utility function that prints detailed information about a Numeric value's internal storage format to stdout.

## Definition
```c
static void dump_numeric(const char *str, Numeric num)
```

## Detailed Description
The `dump_numeric` function is a PostgreSQL debugging utility that provides detailed information about the internal storage format of Numeric values. It prints the weight, scale, sign, and individual digits of a numeric value in a human-readable format. This function is primarily used for development and debugging purposes to understand how numeric values are stored internally.

The function displays the numeric's weight (position of most significant digit), decimal scale (number of digits after decimal point), sign information (including special values like NaN and infinity), and the actual digit array. Each digit is printed with zero-padding according to DEC_DIGITS width.

## Parameters / Member Variables
- `str`: A descriptive string label to prefix the debug output
- `num`: The Numeric value to examine and dump

## Dependencies
- Functions called/Symbols referenced:
  - NUMERIC_DIGITS
  - NUMERIC_NDIGITS
  - NUMERIC_WEIGHT
  - NUMERIC_DSCALE
  - NUMERIC_SIGN
  - NUMERIC_POS
  - NUMERIC_NEG
  - NUMERIC_NAN
  - NUMERIC_PINF
  - NUMERIC_NINF
  - DEC_DIGITS
  - printf (standard C library)
- Called from (representative examples):
  - NUMERIC_ABBREV_NINF
  - [make_result_opt_error](../m/make_result_opt_error.md)

## Notes and Other Information
- Static function - only accessible within numeric.c
- Used for debugging and development purposes
- Handles all numeric sign types including special values (NaN, +/-Infinity)
- Prints digits with zero-padding for consistent formatting
- Output format: \[label\]: NUMERIC w=\[weight\] d=\[scale\] \[sign\] \[digits...\]
- Essential for understanding PostgreSQL's internal numeric representation
- Not part of the public API - intended for internal debugging only