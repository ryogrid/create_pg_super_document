# abstime2tm

## Location
src/interfaces/ecpg/pgtypeslib/dt_common.c: 972 - 1057

## Overview
Converts an AbsoluteTime value to a broken-down time structure (tm), handling timezone information and providing both local time and UTC conversions.

## Definition


## Detailed Description
abstime2tm is a static utility function in the ECPG (Embedded C for PostgreSQL) library that converts PostgreSQL's AbsoluteTime representation to a standard C tm structure. The function provides timezone-aware time conversion, supporting both local time and UTC representations based on the input parameters.

The function operates in two modes:
- **Local time mode** (when tzp is not NULL): Uses localtime() and extracts timezone information
- **UTC mode** (when tzp is NULL): Uses gmtime() for UTC conversion without timezone data

The function handles platform-specific timezone implementations:
- **HAVE_STRUCT_TM_TM_ZONE**: Uses tm_gmtoff and tm_zone fields directly from the tm structure
- **HAVE_INT_TIMEZONE**: Uses global TIMEZONE_GLOBAL and TZNAME_GLOBAL variables
- **Fallback**: Defaults to UTC (timezone offset 0) when no timezone support is available

Error handling includes setting errno to PGTYPES_TS_BAD_TIMESTAMP when time conversion fails, and managing timezone name buffer overflow by setting tm_isdst to -1.

## Parameters / Member Variables
- : Input AbsoluteTime value to be converted
- : Output pointer for timezone offset in seconds from UTC (NULL for UTC conversion)
- : Output tm structure to be filled with broken-down time components
- : Output pointer for timezone name string (can be NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - localtime, gmtime (standard C library time conversion functions)
  - strlcpy (secure string copy function)
  - Constants: PGTYPES_TS_BAD_TIMESTAMP, MAXTZLEN, SECS_PER_HOUR
  - Platform-specific: TIMEZONE_GLOBAL, TZNAME_GLOBAL (when HAVE_INT_TIMEZONE is defined)
  - AbsoluteTime (PostgreSQL's absolute time type)

- Called from (representative examples):
  - GetCurrentDateTime (src/interfaces/ecpg/pgtypeslib/dt_common.c:1062)

## Notes and Other Information
- This is a static function, only accessible within the dt_common.c compilation unit
- The function adjusts standard C tm structure values to match PostgreSQL conventions (year +1900, month +1)
- Timezone name strings are limited to MAXTZLEN characters to prevent buffer overflow
- When timezone name exceeds MAXTZLEN, tm_isdst is set to -1 to indicate an error condition
- The function supports multiple platform-specific timezone implementations for maximum portability
- Used primarily in ECPG for converting PostgreSQL timestamp values to C-compatible time structures
- The negative sign applied to tm_gmtoff compensates for Sun/DEC timezone representation differences