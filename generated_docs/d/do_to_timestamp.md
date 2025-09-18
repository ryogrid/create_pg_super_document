# do_to_timestamp

## Location
src/backend/utils/adt/formatting.c: 4681 - 5037

## Overview
The core datetime parsing function that converts formatted text strings into PostgreSQL's internal time representation structures, serving as the shared implementation for to_timestamp, to_date, and parse_datetime functions.

## Definition
```c
static bool do_to_timestamp(text *date_txt, text *fmt, Oid collid, bool std,
                           struct pg_tm *tm, fsec_t *fsec, struct fmt_tz *tz,
                           int *fprec, uint32 *flags, Node *escontext)
```

## Detailed Description
`do_to_timestamp` is the comprehensive datetime parsing engine in PostgreSQL's formatting system. It performs the complex task of parsing a date/time string according to a specified format pattern and converting it into PostgreSQL's internal time representation structures.

The function operates in several phases:
1. **Format Parsing**: Parses the format string into FormatNode structures, utilizing caching for performance
2. **Input Parsing**: Uses DCH_from_char to parse the input string according to the format nodes
3. **Data Conversion**: Converts parsed components from TmFromChar to standard pg_tm structure
4. **Date/Time Processing**: Handles complex date calculations including Julian dates, ISO weeks, day-of-year conversions
5. **Timezone Processing**: Processes timezone information and converts to GMT offsets
6. **Validation**: Performs comprehensive range checking and validation

The function supports both standard and non-standard parsing modes, implements sophisticated error handling through the escontext mechanism, and handles a wide variety of datetime formats including ISO week dates, Julian dates, and various timezone representations.

## Parameters / Member Variables
- `date_txt` (text*): The input date/time string to be parsed
- `fmt` (text*): The format string specifying how to interpret the input
- `collid` (Oid): Collation ID for string comparison operations
- `std` (bool): Standard parsing mode flag (strict vs. non-strict)
- `tm` (struct pg_tm*): Output structure for parsed date/time components
- `fsec` (fsec_t*): Output for fractional seconds
- `tz` (struct fmt_tz*): Output structure for timezone information
- `fprec` (int*): Output for fractional precision specification (optional)
- `flags` (uint32*): Output for detected datetime component flags (optional)
- `escontext` (Node*): Error context for soft error handling (NULL for exceptions)

## Dependencies
- Functions called/Symbols referenced:
  - `text_to_cstring` - Convert text to C strings
  - `parse_format` - Parse format string into nodes
  - `DCH_cache_fetch` - Retrieve cached format entries
  - `DCH_from_char` - Core character-to-datetime parsing
  - `DCH_datetime_type` - Determine datetime component types
  - `j2date` - Convert Julian day to date components
  - `isoweek2date`, `isoweekdate2date` - ISO week date conversions
  - `isoweek2j` - ISO week to Julian day conversion
  - `isleap` - Leap year detection
  - `ValidateDate` - Date component validation
  - `DateTimeParseError` - Error reporting
  - `DetermineTimeZoneAbbrevOffset` - Timezone abbreviation resolution
  - Various datetime constants (SECS_PER_HOUR, MINS_PER_HOUR, etc.)
- Called from (representative examples):
  - `to_timestamp` - SQL TO_TIMESTAMP function
  - `to_date` - SQL TO_DATE function
  - `parse_datetime` - Dynamic type parsing function

## Notes and Other Information
- The function implements a sophisticated caching mechanism for format strings, using static cache for small formats and dynamic allocation for larger ones
- Supports complex date formats including Julian dates, ISO week dates, and day-of-year specifications
- Handles both 12-hour and 24-hour time formats with proper AM/PM processing
- Implements century and year calculations for various historical date formats
- Provides comprehensive timezone support including fixed offsets, abbreviations, and dynamic timezone resolution
- Uses soft error handling mechanism allowing callers to choose between exceptions and return value checking
- The TmFromChar intermediate structure allows for flexible field processing before final conversion
- Includes extensive validation for all datetime components and timezone specifications
- Memory management is carefully handled with proper cleanup in both success and failure paths
- The function's complexity reflects the rich variety of datetime formats supported by PostgreSQL