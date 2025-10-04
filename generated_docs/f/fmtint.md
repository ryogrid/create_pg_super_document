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
- `value`: The long long integer value to be formatted and output
- `type`: Format specifier character ('d', 'i', 'o', 'u', 'x', 'X') determining output format and base
- `forcesign`: Flag to force output of '+' sign for positive numbers
- `leftjust`: Flag indicating left justification (1 for left-justified, 0 for right-justified)
- `minlen`: Minimum field width; output will be padded to this width if shorter
- `zpad`: Flag indicating zero-padding should be used instead of space padding
- `precision`: Minimum number of digits to output (adds leading zeros if necessary)
- `pointflag`: Flag indicating whether precision was explicitly specified in format string
- `*target`: Output destination structure containing formatting state and output buffer
## Dependencies
- Functions called/Symbols referenced:
  - PrintfTarget (struct type for output destination)
  - [adjust_sign](../a/adjust_sign.md) (helper function for sign processing and determination)
  - Max (PostgreSQL macro for maximum value)
  - [compute_padlen](../c/compute_padlen.md) (helper function to calculate padding requirements)
  - [leading_pad](../l/leading_pad.md) (function to output leading padding and sign)
  - [dopr_outchmulti](../d/dopr_outchmulti.md) (function to output multiple identical characters)
  - [dostr](../d/dostr.md) (function to output string content to target)
  - [trailing_pad](../t/trailing_pad.md) (function to output trailing padding)

- Called from (representative examples):
  - [dopr](../d/dopr.md) (main printf formatting function)
  - [flushbuffer](flushbuffer.md) (output buffer management function)

## Notes and Other Information
- Part of PostgreSQL's platform-independent printf implementation
- Handles special case where precision is 0 and value is 0 (outputs no digits per SUS standard)
- Uses optimized division loops specific to base 8, 10, and 16 for performance
- Includes MSVC-specific pragma to disable warning about applying unary minus to unsigned values
- Uses a 64-byte buffer which is sufficient for the largest possible integer in any supported base
- Builds numeric string representation from right to left in the buffer
- Supports both uppercase (X) and lowercase (x) hexadecimal output
- Integrates with PostgreSQL's padding and alignment system for consistent formatting behavior

## Simplified Source

```c
static void
fmtint(long long value, char type, int forcesign, int leftjust,
       int minlen, int zpad, int precision, int pointflag,
       PrintfTarget *target)
{
    unsigned long long uvalue;
    int base, dosign;
    const char *cvt = "0123456789abcdef";
    int signvalue = 0;
    char convert[64];
    int vallen = 0;
    int padlen, zeropad;

    // Determine base and sign handling based on format type
    switch (type) {
        case 'd': case 'i': base = 10; dosign = 1; break;
        case 'o': base = 8; dosign = 0; break;
        case 'u': base = 10; dosign = 0; break;
        case 'x': base = 16; dosign = 0; break;
        case 'X': cvt = "0123456789ABCDEF"; base = 16; dosign = 0; break;
        default: return;
    }

    // Handle sign and convert to unsigned
    if (dosign && adjust_sign((value < 0), forcesign, &signvalue))
        uvalue = -(unsigned long long) value;
    else
        uvalue = (unsigned long long) value;

    // Convert to string (special case: 0 with precision 0 = empty)
    if (value == 0 && pointflag && precision == 0) {
        vallen = 0;
    } else {
        // Convert using optimized loops for each base
        if (base == 10) {
            do {
                convert[sizeof(convert) - (++vallen)] = cvt[uvalue % 10];
                uvalue = uvalue / 10;
            } while (uvalue);
        } else if (base == 16) {
            do {
                convert[sizeof(convert) - (++vallen)] = cvt[uvalue % 16];
                uvalue = uvalue / 16;
            } while (uvalue);
        } else { // base == 8
            do {
                convert[sizeof(convert) - (++vallen)] = cvt[uvalue % 8];
                uvalue = uvalue / 8;
            } while (uvalue);
        }
    }

    // Apply formatting: precision zeros, padding, output
    zeropad = Max(0, precision - vallen);
    padlen = compute_padlen(minlen, vallen + zeropad, leftjust);

    leading_pad(zpad, signvalue, &padlen, target);
    if (zeropad > 0)
        dopr_outchmulti('0', zeropad, target);
    dostr(convert + sizeof(convert) - vallen, vallen, target);
    trailing_pad(padlen, target);
}
```