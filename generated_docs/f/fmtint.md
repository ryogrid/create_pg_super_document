# fmtint

## Location
[src/port/snprintf.c:1007-1117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/snprintf.c#L1007-L1117)

## Overview
Formats and outputs integer values for various numeric format specifiers (%d, %i, %o, %u, %x, %X) with support for padding, alignment, precision, and sign handling.

## Definition

```c
static void
fmtint(long long value, char type, int forcesign, int leftjust,
	   int minlen, int zpad, int precision, int pointflag,
	   PrintfTarget *target)
```
## Detailed Description
This comprehensive function handles the formatting and output of integer values for all standard integer format specifiers in PostgreSQL's portable snprintf implementation. It supports decimal (%d, %i), octal (%o), unsigned decimal (%u), and hexadecimal (%x, %X) output formats. The function performs base conversion using optimized division operations specific to each base, handles sign processing, applies precision and width formatting, and manages both leading and trailing padding.

The function uses a character buffer to build the string representation from right to left, then applies various formatting rules including zero-padding for precision, space/zero padding for field width, and proper sign handling. It includes special optimizations for common bases (8, 10, 16) to avoid expensive general-purpose division operations.

## Parameters / Member Variables
- : The long long integer value to be formatted and output
- : Format specifier character ('d', 'i', 'o', 'u', 'x', 'X') determining output format and base
- : Flag to force output of '+' sign for positive numbers
- : Flag indicating left justification (1 for left-justified, 0 for right-justified)
- : Minimum field width; output will be padded to this width if shorter
- : Flag indicating zero-padding should be used instead of space padding
- : Minimum number of digits to output (adds leading zeros if necessary)
- : Flag indicating whether precision was explicitly specified in format string
- : Output destination structure containing formatting state and output buffer

## Dependencies
- Functions called/Symbols referenced:
  - PrintfTarget (struct type for output destination)
  - adjust_sign (helper function for sign processing and determination)
  - Max (PostgreSQL macro for maximum value)
  - compute_padlen (helper function to calculate padding requirements)
  - leading_pad (function to output leading padding and sign)
  - dopr_outchmulti (function to output multiple identical characters)
  - dostr (function to output string content to target)
  - trailing_pad (function to output trailing padding)

- Called from (representative examples):
  - dopr (main printf formatting function)
  - flushbuffer (output buffer management function)

## Notes and Other Information
- Part of PostgreSQL's platform-independent printf implementation
- Handles special case where precision is 0 and value is 0 (outputs no digits per SUS standard)
- Uses optimized division loops specific to base 8, 10, and 16 for performance
- Includes MSVC-specific pragma to disable warning about applying unary minus to unsigned values
- Uses a 64-byte buffer which is sufficient for the largest possible integer in any supported base
- Builds numeric string representation from right to left in the buffer
- Supports both uppercase (X) and lowercase (x) hexadecimal output
- Integrates with PostgreSQL's padding and alignment system for consistent formatting behavior