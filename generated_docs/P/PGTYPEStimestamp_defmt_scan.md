# PGTYPEStimestamp_defmt_scan

## Location
src/interfaces/ecpg/pgtypeslib/dt_common.c: 2519 - 3010

## Overview
A comprehensive date/time format string parser that converts formatted string input into PostgreSQL timestamp values according to strftime-style format specifiers.

## Definition
```c
int PGTYPEStimestamp_defmt_scan(char **str, char *fmt, timestamp *d,
                               int *year, int *month, int *day,
                               int *hour, int *minute, int *second,
                               int *tz)
```

## Detailed Description
This function is the core date/time parsing engine for the ECPG pgtypes library. It interprets a wide variety of strftime-style format specifiers to parse formatted date and time strings into their component values. The function supports extensive format codes including:

- Date components: %a/%A (weekday names), %b/%B/%h (month names), %C (century), %d/%e (day), %m (month), %y/%g/%G/%Y (year variations)
- Time components: %H/%I/%k/%l (hour variations), %M (minute), %S (second), %p/%P (AM/PM indicators)
- Special formats: %D (MM/DD/YY), %r (12-hour time), %R (24-hour time), %T (time), %s (Unix timestamp)
- Week and timezone: %j (day of year), %u/%U/%V/%w/%W (week-related), %z/%Z (timezone)
- Literals: %n (newline), %t (tab), %% (percent sign)

The function recursively handles composite format specifiers (like %D, %r, %R, %T) by expanding them into their constituent parts. It performs comprehensive validation of parsed values and constructs a final timestamp using tm2timestamp().

## Parameters / Member Variables
- `str`: Pointer to input string pointer (modified to track parsing position)
- `fmt`: Format string with strftime-style specifiers defining expected input structure
- `d`: Output timestamp value constructed from parsed components
- `year`: Pointer to store parsed year value
- `month`: Pointer to store parsed month value (1-12)
- `day`: Pointer to store parsed day value (1-31)
- `hour`: Pointer to store parsed hour value (0-24)
- `minute`: Pointer to store parsed minute value (0-59)
- `second`: Pointer to store parsed second value (0-59)
- `tz`: Pointer to store parsed timezone offset in seconds

## Dependencies
- Functions called/Symbols referenced:
  - [pgtypes_defmt_scan](../p/pgtypes_defmt_scan.md) (extensively for individual component parsing)
  - [pgtypes_alloc](../p/pgtypes_alloc.md) (for temporary string allocation)
  - [DecodeTimezone](../D/DecodeTimezone.md) (for timezone string parsing)
  - [tm2timestamp](../t/tm2timestamp.md) (for final timestamp construction)
  - strncmp (standard C library function)
  - strlen (standard C library function)
  - strcpy/strcat (standard C library functions)
  - gmtime (standard C library function)
  - free (standard C library function)
  - [pg_strcasecmp](../p/pg_strcasecmp.md) (PostgreSQL string comparison)
  - isleap (leap year checking)
  - Various constants: PGTYPES_TYPE_UINT, PGTYPES_TYPE_UINT_LONG, PGTYPES_TYPE_STRING_MALLOCED, MONTHS_PER_YEAR, TZ, DTZ
  - Global arrays: pgtypes_date_weekdays_short, days, months, pgtypes_date_months, datetktbl, day_tab
- Called from (representative examples):
  - [PGTYPEStimestamp_defmt_asc](PGTYPEStimestamp_defmt_asc.md)
  - Self-recursively for composite format specifiers

## Notes and Other Information
- Returns 0 on success, 1 on error
- Handles whitespace flexibly by skipping spaces in both input and format strings
- Supports both short and long forms of weekday/month names
- Implements AM/PM time conversion with multiple formats (am/pm, a.m./p.m., AM/PM, A.M./P.M.)
- Validates all parsed values against reasonable ranges and adjusts invalid values
- Uses recursive parsing for composite format specifiers like %D, %r, %R, %T
- Handles 2-digit year interpretation (adds 1900 if year < 100)
- Part of the ECPG pgtypes library for embedded SQL date/time processing
- Located in src/interfaces/ecpg/pgtypeslib/dt_common.c:2519-3010
- Contains XXX comments indicating areas for potential future enhancement
- Supports Unix timestamp parsing via %s format specifier