# dump_var

## Location
src/backend/utils/adt/numeric.c: 6916 - 6968

## Overview
A debugging utility function that prints detailed information about a NumericVar value's internal variable format to stdout.

## Definition
```c
static void dump_var(const char *str, NumericVar *var)
```

## Detailed Description
The `dump_var` function is a PostgreSQL debugging utility that provides detailed information about the internal variable format representation of numeric values. It prints the weight, display scale, sign, and individual digits of a NumericVar structure in a human-readable format. This function is primarily used for development and debugging purposes to understand how numeric values are represented in PostgreSQL's internal working format.

Unlike `dump_numeric` which works with the storage format, `dump_var` operates on NumericVar structures which are used during arithmetic operations. The function displays the variable's weight, display scale (dscale), sign information (including special values), and the digit array with appropriate formatting.

## Parameters / Member Variables
- `str`: A descriptive string label to prefix the debug output
- `var`: Pointer to the NumericVar structure to examine and dump

## Dependencies
- Functions called/Symbols referenced:
  - NUMERIC_POS
  - NUMERIC_NEG
  - NUMERIC_NAN
  - NUMERIC_PINF
  - NUMERIC_NINF
  - DEC_DIGITS
  - printf (standard C library)
- Called from (representative examples):
  - NUMERIC_ABBREV_NINF

## Notes and Other Information
- Static function - only accessible within numeric.c
- Used for debugging NumericVar structures during development
- Handles all numeric sign types including special values (NaN, +/-Infinity)
- Prints digits with zero-padding for consistent formatting using DEC_DIGITS
- Output format: \[label\]: VAR w=\[weight\] d=\[dscale\] \[sign\] \[digits...\]
- Complementary to dump_numeric() - operates on variable format vs storage format
- Essential for debugging numeric arithmetic operations
- NumericVar is the internal working format used during calculations
- Not part of the public API - intended for internal debugging only