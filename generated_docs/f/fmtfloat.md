# fmtfloat

## Location
[src/port/snprintf.c:1136-1281](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/snprintf.c#L1136-L1281)

## Overview
Formats floating-point numbers for printf-style output, handling special cases like NaN, infinity, and platform-specific formatting requirements while supporting various precision and padding options.

## Definition

```c
static void
fmtfloat(double value, char type, int forcesign, int leftjust,
		 int minlen, int zpad, int precision, int pointflag,
		 PrintfTarget *target)
```
## Detailed Description
The  function is a core component of PostgreSQL's portable snprintf implementation that handles the formatting of floating-point numbers. It uses the system's snprintf for basic conversion but adds comprehensive handling for:

- Special values (NaN, positive/negative infinity, negative zero)
- Cross-platform consistency (especially Windows exponent formatting)
- Precision control with overflow protection (max 350 digits, with additional zero padding)
- Sign handling including IEEE minus zero detection
- Padding and alignment according to format specifications

The function uses a 1024-byte buffer to handle the extreme range of double values (approximately 1E±308) and implements safeguards against buffer overflow by limiting precision requests.

## Parameters / Member Variables
- : The double-precision floating-point number to format
- : Format type character ('f', 'e', 'E', 'g', 'G')
- : Flag to force display of positive sign
- : Flag for left justification in field width
- : Minimum field width for output
- : Flag to pad with zeros instead of spaces
- : Number of decimal places or significant digits
- : Flag indicating if precision was explicitly specified
- : Output target structure to receive formatted result

## Dependencies
- Functions called/Symbols referenced:
  - [adjust_sign](../a/adjust_sign.md)
  - [compute_padlen](../c/compute_padlen.md)
  - [leading_pad](../l/leading_pad.md)
  - [trailing_pad](../t/trailing_pad.md)
  - [dostr](../d/dostr.md)
  - [dopr_outchmulti](../d/dopr_outchmulti.md)
  - isnan
  - isinf
  - snprintf (system library)
- Called from:
  - [dopr](../d/dopr.md)
  - [flushbuffer](flushbuffer.md)

## Notes and Other Information
- Handles IEEE floating-point special cases for cross-platform consistency
- Implements Windows-specific hack to normalize three-digit exponents to two digits
- Uses memcmp to detect IEEE minus zero (where value == 0.0 but bit pattern differs)
- Limits precision to 350 digits to prevent buffer overflow, padding with additional zeros if needed
- Buffer size of 1024 bytes accommodates extreme floating-point ranges
- Part of PostgreSQL's portable printf implementation in src/port/snprintf.c