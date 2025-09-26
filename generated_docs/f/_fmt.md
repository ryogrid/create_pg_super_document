# _fmt

## Location
src/timezone/strftime.c: 151 - 515

## Overview
Core internal formatting function that processes strftime format specifiers and converts timestamp components into their string representations according to POSIX and C99 standards.

## Definition


## Detailed Description
_fmt is the heart of PostgreSQL's strftime implementation, responsible for parsing format strings and converting timestamp components into formatted output. It processes each character in the format string, handling literal characters by copying them directly and format specifiers (prefixed with %) by converting the corresponding timestamp field using appropriate formatting.

The function implements a comprehensive set of format specifiers including POSIX standard ones (%Y, %m, %d, etc.) and extensions (%F for ISO date, %T for time, etc.). It handles complex cases like ISO 8601 week numbering (%V, %G, %g) and timezone formatting (%z, %Z). The function is recursive, as some format specifiers expand to other format strings that are processed by calling _fmt again.

Error handling includes bounds checking to prevent buffer overflows and warning propagation for potentially problematic conversions like 2-digit years. The function maintains locale awareness through the global Locale structure for month names, day names, and other locale-specific formatting.

## Parameters / Member Variables
- : Format string containing literal text and % format specifiers
- : Pointer to pg_tm structure containing the timestamp components to format
- : Current position in the output buffer where formatted text should be written
- : Pointer to the end of the output buffer (exclusive limit)
- : Pointer to warning level that tracks potential formatting issues

## Dependencies
- Functions called/Symbols referenced:
  - _add (adds strings to output buffer)
  - _conv (converts integers to formatted strings)
  - _yconv (converts years with century handling)
  - isleap_sum (checks for leap years)
  - Locale (global locale information structure)
  - Various constants: DAYSPERWEEK, MONSPERYEAR, TM_YEAR_BASE, etc.
- Called from (representative examples):
  - pg_strftime (main entry point)
  - _fmt (recursive calls for composite format specifiers)

## Notes and Other Information
- Supports extensive format specifiers including POSIX standard and common extensions
- Handles ISO 8601 week date calculations (%V, %G, %g) with complex logic for year boundaries
- Implements locale-aware formatting for month/day names and AM/PM indicators
- Uses recursive approach for composite format specifiers like %c, %D, %F, %R, %r, %T
- Includes historical compatibility notes and comments about format specifier changes
- Contains optional KITCHEN_SINK feature for %K specifier (humorous Easter egg)
- Warning system tracks potentially problematic conversions like 2-digit years
- Buffer overflow protection through ptlim boundary checking