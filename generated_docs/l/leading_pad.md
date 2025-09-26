# leading_pad

## Location
src/port/snprintf.c: 1492 - 1527

## Overview
Handles the output of leading padding and sign characters for formatted numeric values, supporting both zero-padding and space-padding with proper sign placement.

## Definition
```c
static void leading_pad(int zpad, int signvalue, int *padlen, PrintfTarget *target)
```

## Detailed Description
The `leading_pad` function is responsible for outputting the appropriate leading padding and sign characters when formatting numeric values in PostgreSQL's custom sprintf implementation. It handles the complex logic of placing sign characters correctly relative to zero-padding and space-padding.

When zero-padding is requested, the function outputs the sign character first, then the zero padding. For space-padding, it outputs spaces first, leaving room for the sign character to be placed just before the numeric digits. This ensures that formatted numbers appear correctly with signs in the right position relative to padding characters.

The function modifies the padlen parameter to track how much padding has been consumed, allowing subsequent formatting functions to know how much additional formatting work remains.

## Parameters / Member Variables
- `zpad`: The zero-padding character ('0') when zero-padding is requested, or 0 for space-padding
- `signvalue`: The sign character ('+' or '-') to be output, or 0 if no sign is needed
- `padlen`: Pointer to the remaining padding length; modified by the function to reflect consumed padding
- `target`: Pointer to the PrintfTarget structure that handles the actual character output

## Dependencies
- Functions called/Symbols referenced:
  - PrintfTarget (structure used for output handling)
  - dopr_outch (outputs a single character)
  - dopr_outchmulti (outputs multiple copies of a character)
- Called from (representative examples):
  - flushbuffer (at src/port/snprintf.c:336)
  - fmtint (at src/port/snprintf.c:1107)
  - fmtfloat (at src/port/snprintf.c:1236)

## Notes and Other Information
- This is a static function within the snprintf.c module, indicating it's an internal utility
- Handles the complex interaction between sign placement and padding for format specifiers like `%+08d`
- Ensures that zero-padding appears between the sign and the digits (e.g., `+0000123`)
- For space-padding, reserves space for the sign and places it just before the digits
- Modifies the padlen parameter in-place to communicate remaining padding to caller
- Part of PostgreSQL's portable snprintf implementation that provides consistent formatting across platforms
- The function's logic ensures proper formatting for both positive and negative numbers with various padding requirements