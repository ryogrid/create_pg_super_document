# do_to_timestamp

## Location
[src/backend/utils/adt/formatting.c:4681-5037](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L4681-L5037)

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
  - `[text_to_cstring](../t/text_to_cstring.md)` - Convert text to C strings
  - `[parse_format](../p/parse_format.md)` - Parse format string into nodes
  - `[DCH_cache_fetch](../D/DCH_cache_fetch.md)` - Retrieve cached format entries
  - [DCH_from_char](../D/DCH_from_char.md) - Core character-to-datetime parsing
  - `[DCH_datetime_type](../D/DCH_datetime_type.md)` - Determine datetime component types
  - [j2date](../j/j2date.md) - Convert Julian day to date components
  - [isoweek2date](../i/isoweek2date.md), `isoweekdate2date` - ISO week date conversions
  - [isoweek2j](../i/isoweek2j.md) - ISO week to Julian day conversion
  - `isleap` - Leap year detection
  - `[ValidateDate](../V/ValidateDate.md)` - Date component validation
  - `[DateTimeParseError](../D/DateTimeParseError.md)` - Error reporting
  - [DetermineTimeZoneAbbrevOffset](../D/DetermineTimeZoneAbbrevOffset.md) - Timezone abbreviation resolution
  - Various datetime constants (SECS_PER_HOUR, MINS_PER_HOUR, etc.)
- Called from (representative examples):
  - [to_timestamp](../t/to_timestamp.md) - SQL TO_TIMESTAMP function
  - [to_date](../t/to_date.md) - SQL TO_DATE function
  - [parse_datetime](../p/parse_datetime.md) - Dynamic type parsing function

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

## Simplified Source

```c
static bool do_to_timestamp(text *date_txt, text *fmt, Oid collid, bool std,
                           struct pg_tm *tm, fsec_t *fsec, struct fmt_tz *tz,
                           int *fprec, uint32 *flags, Node *escontext) {
    FormatNode *format = NULL;
    TmFromChar tmfc;
    char *date_str = text_to_cstring(date_txt);

    // Initialize output structures
    ZERO_tmfc(&tmfc);
    ZERO_tm(tm);
    *fsec = 0;
    tz->has_tz = false;

    // Parse format string into nodes
    if (VARSIZE_ANY_EXHDR(fmt) > 0) {
        char *fmt_str = text_to_cstring(fmt);

        // Use cache or allocate format nodes based on size
        if (fmt_len > DCH_CACHE_SIZE) {
            format = palloc((fmt_len + 1) * sizeof(FormatNode));
            parse_format(format, fmt_str, DCH_keywords, DCH_suff, DCH_index,
                        DCH_FLAG | (std ? STD_FLAG : 0), NULL);
        } else {
            DCHCacheEntry *ent = DCH_cache_fetch(fmt_str, std);
            format = ent->format;
        }

        // Parse input string according to format
        DCH_from_char(format, date_str, &tmfc, collid, std, escontext);
        if (SOFT_ERROR_OCCURRED(escontext)) goto fail;
    }

    // Convert parsed fields to standard tm structure
    // Handle time components (hours, minutes, seconds)
    if (tmfc.ssss) {
        tm->tm_hour = tmfc.ssss / SECS_PER_HOUR;
        tm->tm_min = (tmfc.ssss % SECS_PER_HOUR) / SECS_PER_MINUTE;
        tm->tm_sec = tmfc.ssss % SECS_PER_MINUTE;
    }
    if (tmfc.hh) tm->tm_hour = tmfc.hh;
    if (tmfc.mi) tm->tm_min = tmfc.mi;
    if (tmfc.ss) tm->tm_sec = tmfc.ss;

    // Handle 12-hour clock conversion
    if (tmfc.clock == CLOCK_12_HOUR) {
        if (tmfc.pm && tm->tm_hour < 12) tm->tm_hour += 12;
        else if (!tmfc.pm && tm->tm_hour == 12) tm->tm_hour = 0;
    }

    // Handle year/century calculations
    if (tmfc.year) {
        tm->tm_year = tmfc.year;
        if (tmfc.bc) tm->tm_year = -tm->tm_year + 1;
    }

    // Handle various date formats (Julian, ISO week, day-of-year)
    if (tmfc.j) {
        j2date(tmfc.j, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
    } else if (tmfc.ww && tmfc.mode == FROM_CHAR_DATE_ISOWEEK) {
        if (tmfc.d)
            isoweekdate2date(tmfc.ww, tmfc.d, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
        else
            isoweek2date(tmfc.ww, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
    } else {
        if (tmfc.dd) tm->tm_mday = tmfc.dd;
        if (tmfc.mm) tm->tm_mon = tmfc.mm;
    }

    // Handle fractional seconds
    if (tmfc.ms) *fsec += tmfc.ms * 1000;
    if (tmfc.us) *fsec += tmfc.us;

    // Handle timezone information
    if (tmfc.tzsign) {
        tz->has_tz = true;
        tz->gmtoffset = (tmfc.tzh * MINS_PER_HOUR + tmfc.tzm) * SECS_PER_MINUTE;
        if (tmfc.tzsign > 0) tz->gmtoffset = -tz->gmtoffset;
    }

    // Validate all components
    if (ValidateDate(fmask, true, false, false, tm) != 0) {
        DateTimeParseError(DTERR_FIELD_OVERFLOW, NULL, date_str, "timestamp", escontext);
        goto fail;
    }

    pfree(date_str);
    return true;

fail:
    pfree(date_str);
    return false;
}
```