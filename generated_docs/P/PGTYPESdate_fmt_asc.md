# PGTYPESdate_fmt_asc

## Location
[src/interfaces/ecpg/pgtypeslib/datetime.c:168-327](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/datetime.c#L168-L327)

## Overview
Formats a Julian date according to a specified format string, producing a human-readable date representation.

## Definition
```c
int PGTYPESdate_fmt_asc(date dDate, const char *fmtstring, char *outbuf)
```

## Detailed Description
This function converts a Julian date to a formatted string representation by parsing a format string and replacing format specifiers with actual date components. The function supports various format patterns including day names, numeric days, month names, numeric months, and years in different formats. It uses a pattern matching approach where format specifiers are replaced in order of decreasing length to avoid conflicts. The function converts the Julian date to standard date components, determines the day of the week, and then processes the format string by replacing each recognized pattern with the appropriate formatted value.

## Parameters / Member Variables
- `dDate`: Julian date value to be formatted
- `fmtstring`: Format string containing patterns to be replaced (e.g., "yyyy-mm-dd", "ddd, mmm dd, yyyy")
- `outbuf`: Output buffer where the formatted date string will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [j2date](../j/j2date.md): Converts Julian day number to year/month/day components
  - [date2j](../d/date2j.md): Converts year/month/day to Julian day number (for reference date)
  - [PGTYPESdate_dayofweek](PGTYPESdate_dayofweek.md): Determines day of week for the date
  - [pgtypes_alloc](../p/pgtypes_alloc.md): Allocates memory for temporary string formatting
  - strcpy, strstr, memcpy: String manipulation functions
- Format constants used:
  - PGTYPES_FMTDATE_DOW_LITERAL_SHORT: Day of week abbreviation ("ddd")
  - PGTYPES_FMTDATE_DAY_DIGITS_LZ: Day with leading zero ("dd")
  - PGTYPES_FMTDATE_MONTH_LITERAL_SHORT: Month abbreviation ("mmm")
  - PGTYPES_FMTDATE_MONTH_DIGITS_LZ: Month with leading zero ("mm")
  - PGTYPES_FMTDATE_YEAR_DIGITS_LONG: Four-digit year ("yyyy")
  - PGTYPES_FMTDATE_YEAR_DIGITS_SHORT: Two-digit year ("yy")
- Called from (representative examples):
  - [rfmtdate](../r/rfmtdate.md): Informix compatibility wrapper function
  - [main](../m/main.md): Used in test programs (dt_test.c)

## Notes and Other Information
- Returns 0 on success, -1 on memory allocation failure
- Part of PostgreSQL's ECPG (Embedded SQL in C) pgtypes library
- Format patterns are processed in order of decreasing length to prevent substring conflicts
- Supports multiple format types: string constants, zero-padded integers of various widths
- Uses static arrays for day and month names (pgtypes_date_weekdays_short, months)
- Memory management includes proper cleanup of dynamically allocated strings
- The output buffer must be large enough to contain the formatted result
- Essential for displaying dates in user-friendly formats in embedded SQL applications
- Format string is copied to output buffer first, then patterns are replaced in-place