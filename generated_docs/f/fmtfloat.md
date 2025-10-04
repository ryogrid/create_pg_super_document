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
- `value`: The double-precision floating-point number to format
- `type`: Format type character ('f', 'e', 'E', 'g', 'G')
- `forcesign`: Flag to force display of positive sign
- `leftjust`: Flag for left justification in field width
- `minlen`: Minimum field width for output
- `zpad`: Flag to pad with zeros instead of spaces
- `precision`: Number of decimal places or significant digits
- `pointflag`: Flag indicating if precision was explicitly specified
- `*target`: Output target structure to receive formatted result
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

## Simplified Source

```c
static void
fmtfloat(double value, char type, int forcesign, int leftjust,
         int minlen, int zpad, int precision, int pointflag,
         PrintfTarget *target)
{
    int signvalue = 0;
    int prec, vallen;
    char fmt[8];
    char convert[1024];
    int zeropadlen = 0;
    int padlen;

    // Limit precision to prevent buffer overflow
    if (precision < 0) precision = 0;
    prec = Min(precision, 350);

    // Handle special cases
    if (isnan(value)) {
        strcpy(convert, "NaN");
        vallen = 3;
    } else {
        // Handle sign (including IEEE minus zero)
        static const double dzero = 0.0;
        if (adjust_sign((value < 0.0 ||
                        (value == 0.0 && memcmp(&value, &dzero, sizeof(double)) != 0)),
                       forcesign, &signvalue))
            value = -value;

        if (isinf(value)) {
            strcpy(convert, "Infinity");
            vallen = 8;
        } else {
            // Format the number using system snprintf
            zeropadlen = precision - prec;
            if (pointflag) {
                sprintf(fmt, "%%.%d%c", prec, type);
            } else {
                sprintf(fmt, "%%%c", type);
            }
            vallen = snprintf(convert, sizeof(convert), fmt, value);
            if (vallen < 0) {
                target->failed = true;
                return;
            }

#ifdef WIN32
            // Fix Windows three-digit exponent format
            if (vallen >= 6 && convert[vallen-5] == 'e' && convert[vallen-3] == '0') {
                convert[vallen-3] = convert[vallen-2];
                convert[vallen-2] = convert[vallen-1];
                vallen--;
            }
#endif
        }
    }

    // Apply padding and output
    padlen = compute_padlen(minlen, vallen + zeropadlen, leftjust);
    leading_pad(zpad, signvalue, &padlen, target);

    // Handle precision zero-padding (inject before exponent if present)
    if (zeropadlen > 0) {
        char *epos = strrchr(convert, 'e');
        if (!epos) epos = strrchr(convert, 'E');

        if (epos) {
            dostr(convert, epos - convert, target);
            dopr_outchmulti('0', zeropadlen, target);
            dostr(epos, vallen - (epos - convert), target);
        } else {
            dostr(convert, vallen, target);
            dopr_outchmulti('0', zeropadlen, target);
        }
    } else {
        dostr(convert, vallen, target);
    }

    trailing_pad(padlen, target);
}
```