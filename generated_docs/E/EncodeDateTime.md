# EncodeDateTime

## Location
[src/interfaces/ecpg/pgtypeslib/dt_common.c:753-948](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/dt_common.c#L753-L948)

## Overview
Encodes date and time information as a formatted string according to various international and database-specific styles, with optional timezone support.

## Definition

```c
void
EncodeDateTime(struct tm *tm, fsec_t fsec, bool print_tz, int tz, const char *tzn, int style, char *str, bool EuroDates)
```
## Detailed Description
EncodeDateTime is a comprehensive datetime formatting function that converts PostgreSQL's internal time representation into human-readable strings. It supports multiple international date/time formats and handles timezone information when specified.

The function supports these major formatting styles:
- **USE_ISO_DATES**: ISO 8601 format (YYYY-MM-DD HH:MM:SS±TZ) with space separator
- **USE_XSD_DATES**: XML Schema format (YYYY-MM-DDTHH:MM:SS.SS±TZ) with 'T' separator  
- **USE_SQL_DATES**: Oracle/Ingres format (MM/DD/YYYY HH:MM:SS.SS TZ or DD/MM/YYYY based on DateOrder)
- **USE_GERMAN_DATES**: German format (DD.MM.YYYY HH:MM:SS TZ)
- **USE_POSTGRES_DATES**: Traditional PostgreSQL format (Dow Mon DD HH:MM:SS.SS YYYY TZ)

The function handles BC dates (years ≤ 0), microsecond precision, timezone abbreviations, and numeric timezone offsets. It respects the global DateOrder setting for day/month ordering in applicable formats.

## Parameters / Member Variables
- : Pointer to pg_tm structure containing date/time components (year, month, day, hour, minute, second, etc.)
- : Fractional seconds (microseconds) component
- : Boolean flag determining whether to include timezone information in output
- : Numeric timezone offset in seconds from UTC (used when tzn is not provided)
- : Textual timezone abbreviation (e.g., "EST", "PST") - takes precedence over tz when provided
- : Integer constant specifying the output format style
- : Output buffer where the formatted datetime string will be written

## Dependencies
- Functions called/Symbols referenced:
  - [pg_ultostr_zeropad](../p/pg_ultostr_zeropad.md) (zero-padded number formatting)
  - AppendTimestampSeconds (seconds and fractional seconds formatting)
  - EncodeTimezone (timezone offset formatting)
  - [date2j](../d/date2j.md), j2day (Julian date conversion for day-of-week calculation)
  - sprintf, strlen, memcpy (standard C library functions)
  - Various constants: MONTHS_PER_YEAR, MAXTZLEN, DateOrder, DATEORDER_DMY
  - Style constants: USE_ISO_DATES, USE_XSD_DATES, USE_SQL_DATES, USE_GERMAN_DATES, USE_POSTGRES_DATES
  - Global arrays: days[], months[] (day/month name abbreviations)

- Called from (representative examples):
  - [timestamp_out](../t/timestamp_out.md) (src/backend/utils/adt/timestamp.c:244)
  - [timestamptz_out](../t/timestamptz_out.md) (src/backend/utils/adt/timestamp.c:799)
  - JsonEncodeDateTime (src/backend/utils/adt/json.c:362, 400)
  - [map_sql_value_to_xml_value](../m/map_sql_value_to_xml_value.md) (src/backend/utils/adt/xml.c:2579, 2606)
  - [PGTYPEStimestamp_to_asc](../P/PGTYPEStimestamp_to_asc.md) (src/interfaces/ecpg/pgtypeslib/timestamp.c:284)

## Notes and Other Information
- The function includes validation that tm_mon is within valid range (1-12)
- When tm_isdst < 0 (no valid timezone translation), timezone printing is disabled
- BC dates are handled with special year adjustment formula: -(tm_year - 1)
- Timezone abbreviations are limited to MAXTZLEN characters and assumed to be ASCII
- The PostgreSQL style calculates day-of-week using Julian date functions
- All timezone abbreviations in the IANA database are plain ASCII, making the %.*s formatting safe
- The function modifies the output buffer in-place and null-terminates the result
- DateOrder global setting affects SQL and PostgreSQL style date component ordering