# pg_strfromd

## Location
[src/port/snprintf.c:1282-1373](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/snprintf.c#L1282-L1373)

## Overview
A PostgreSQL-specific function that efficiently formats double-precision floating-point numbers to strings, designed as an optimized alternative to strfromd() with an API tailored for PostgreSQL's float output needs.

## Definition

```c
int
pg_strfromd(char *str, size_t count, int precision, double value)
```
## Detailed Description
The  function provides a streamlined interface for converting double values to string representation, specifically designed for PostgreSQL's float8out() function. It behaves similarly to snprintf() with a format of "%.ng" where n is the specified precision, but with several optimizations:

- Uses a simplified version of fmtfloat() logic without padding support
- Bounds precision to a reasonable range (1-32) for safety and efficiency
- Uses a smaller 64-byte conversion buffer since no padding is needed
- Handles special values (NaN, infinity, negative zero) consistently
- Applies Windows-specific exponent formatting normalization
- Returns the number of characters written or -1 on failure

The function is part of PostgreSQL's portable printf implementation and provides better performance for floating-point output compared to general-purpose formatting functions.

## Parameters / Member Variables
- `*str`: Destination buffer to receive the formatted string
- `count`: Size of the destination buffer (must be > 0)
- `precision`: Number of significant digits (bounded to 1-32 range)
- `value`: The double-precision floating-point value to format
## Dependencies
- Functions called/Symbols referenced:
  - [dopr_outch](../d/dopr_outch.md)
  - [dostr](../d/dostr.md)
  - isnan
  - isinf
  - snprintf (system library)
  - PrintfTarget (structure)
- Called from:
  - [float4out](../f/float4out.md)
  - [float8out_internal](../f/float8out_internal.md)
  - printf (via include/port.h)

## Notes and Other Information
- Designed specifically for PostgreSQL's float output functions for optimal performance
- Always uses 'g' format specifier for automatic selection between fixed and exponential notation
- Buffer must be non-empty (count > 0) - this is asserted at runtime
- Precision is silently clamped to 1-32 range to prevent buffer overflow and ensure reasonable output
- Handles IEEE minus zero detection using memcmp comparison
- Applies same Windows exponent normalization as fmtfloat() for cross-platform consistency
- Returns total character count including any that might have been written to stream (though stream is NULL in this function)

## Simplified Source

```c
int
pg_strfromd(char *str, size_t count, int precision, double value)
{
    PrintfTarget target;
    int signvalue = 0;
    int vallen;
    char fmt[8];
    char convert[64];

    // Initialize output target
    Assert(count > 0);
    target.bufstart = target.bufptr = str;
    target.bufend = str + count - 1;
    target.stream = NULL;
    target.nchars = 0;
    target.failed = false;

    // Bound precision to reasonable range (1-32)
    if (precision < 1) precision = 1;
    else if (precision > 32) precision = 32;

    // Handle special cases
    if (isnan(value)) {
        strcpy(convert, "NaN");
        vallen = 3;
    } else {
        // Handle sign (including IEEE minus zero)
        static const double dzero = 0.0;
        if (value < 0.0 || (value == 0.0 && memcmp(&value, &dzero, sizeof(double)) != 0)) {
            signvalue = '-';
            value = -value;
        }

        if (isinf(value)) {
            strcpy(convert, "Infinity");
            vallen = 8;
        } else {
            // Format using 'g' specifier with specified precision
            sprintf(fmt, "%%.%dg", precision);
            vallen = snprintf(convert, sizeof(convert), fmt, value);
            if (vallen < 0) {
                target.failed = true;
                goto fail;
            }

#ifdef WIN32
            // Normalize Windows three-digit exponents
            if (vallen >= 6 && convert[vallen-5] == 'e' && convert[vallen-3] == '0') {
                convert[vallen-3] = convert[vallen-2];
                convert[vallen-2] = convert[vallen-1];
                vallen--;
            }
#endif
        }
    }

    // Output sign and formatted number
    if (signvalue)
        dopr_outch(signvalue, &target);
    dostr(convert, vallen, &target);

fail:
    *(target.bufptr) = '\0';
    return target.failed ? -1 : (target.bufptr - target.bufstart + target.nchars);
}
```