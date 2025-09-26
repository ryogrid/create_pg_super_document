# _conv

## Location
[src/timezone/strftime.c:516-524](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/strftime.c#L516-L524)

## Overview
Utility function that converts an integer value to its formatted string representation using sprintf and adds it to the output buffer.

## Definition

```c
static char *
_conv(int n, const char *format, char *pt, const char *ptlim)
```
## Detailed Description
_conv is a simple but essential helper function in PostgreSQL's strftime implementation that handles the conversion of numeric values to their string representations. It takes an integer value and a printf-style format specifier, formats the number using sprintf into a temporary buffer, and then adds the result to the output buffer using the _add function.

The function uses a statically-sized buffer calculated by INT_STRLEN_MAXIMUM to ensure it can hold any possible integer value plus a null terminator. This approach provides safety against buffer overflows while maintaining efficiency by avoiding dynamic memory allocation.

_conv is extensively used throughout the _fmt function to format various timestamp components like day numbers, month numbers, hours, minutes, seconds, and other numeric fields that appear in strftime format specifiers.

## Parameters / Member Variables
- : Integer value to be converted to string
- : Printf-style format string specifying how the integer should be formatted (e.g., "%02d", "%2d", "%03d")
- : Current position in the output buffer where the formatted string should be added
- : Pointer to the end of the output buffer (exclusive limit for bounds checking)

## Dependencies
- Functions called/Symbols referenced:
  - sprintf (standard C library function for string formatting)
  - _add (adds the formatted string to the output buffer)
  - INT_STRLEN_MAXIMUM (macro defining maximum string length for an integer)
- Called from (representative examples):
  - _fmt (for formatting day numbers %d, %e)
  - _fmt (for formatting month numbers %m)
  - _fmt (for formatting hours %H, %I, %k, %l)
  - _fmt (for formatting minutes %M, seconds %S)
  - _fmt (for formatting week numbers %U, %W, %V)
  - _fmt (for formatting timezone offsets %z)

## Notes and Other Information
- Uses a fixed-size buffer sized to handle the maximum possible integer string length
- Provides a clean abstraction for numeric formatting within the strftime system
- The temporary buffer approach ensures thread safety as each call uses its own stack-allocated buffer
- Essential building block for most numeric format specifiers in strftime
- Format string parameter allows flexible numeric formatting (zero-padding, field width, etc.)