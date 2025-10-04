# abstime2tm

## Location
[src/interfaces/ecpg/pgtypeslib/dt_common.c:972-1057](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/dt_common.c#L972-L1057)

## Overview
Converts an AbsoluteTime value to a broken-down time structure (tm), handling timezone information and providing both local time and UTC conversions.

## Definition

```c
static void
abstime2tm(AbsoluteTime _time, int *tzp, struct tm *tm, char **tzn)
```
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
- `_time`: Input AbsoluteTime value to be converted
- `*tzp`: Output pointer for timezone offset in seconds from UTC (NULL for UTC conversion)
- `*tm`: Output tm structure to be filled with broken-down time components
- `**tzn`: Output pointer for timezone name string (can be NULL if not needed)
## Dependencies
- Functions called/Symbols referenced:
  - localtime, gmtime (standard C library time conversion functions)
  - [strlcpy](../s/strlcpy.md) (secure string copy function)
  - Constants: PGTYPES_TS_BAD_TIMESTAMP, MAXTZLEN, SECS_PER_HOUR
  - Platform-specific: TIMEZONE_GLOBAL, TZNAME_GLOBAL (when HAVE_INT_TIMEZONE is defined)
  - AbsoluteTime (PostgreSQL's absolute time type)

- Called from (representative examples):
  - [GetCurrentDateTime](../G/GetCurrentDateTime.md) (src/interfaces/ecpg/pgtypeslib/dt_common.c:1062)

## Notes and Other Information
- This is a static function, only accessible within the dt_common.c compilation unit
- The function adjusts standard C tm structure values to match PostgreSQL conventions (year +1900, month +1)
- Timezone name strings are limited to MAXTZLEN characters to prevent buffer overflow
- When timezone name exceeds MAXTZLEN, tm_isdst is set to -1 to indicate an error condition
- The function supports multiple platform-specific timezone implementations for maximum portability
- Used primarily in ECPG for converting PostgreSQL timestamp values to C-compatible time structures
- The negative sign applied to tm_gmtoff compensates for Sun/DEC timezone representation differences

## Simplified Source

```c
static void
abstime2tm(AbsoluteTime _time, int *tzp, struct tm *tm, char **tzn)
{
    time_t time = (time_t) _time;
    struct tm *tx;

    // Get time structure (local time if tzp given, UTC otherwise)
    if (tzp != NULL)
        tx = localtime(&time);
    else
        tx = gmtime(&time);

    if (!tx) {
        errno = PGTYPES_TS_BAD_TIMESTAMP;
        return;
    }

    // Copy time fields, adjusting year and month to PostgreSQL conventions
    tm->tm_year = tx->tm_year + 1900;
    tm->tm_mon = tx->tm_mon + 1;
    tm->tm_mday = tx->tm_mday;
    tm->tm_hour = tx->tm_hour;
    tm->tm_min = tx->tm_min;
    tm->tm_sec = tx->tm_sec;
    tm->tm_isdst = tx->tm_isdst;

    // Handle timezone information based on platform capabilities
    if (tzp != NULL) {
#if defined(HAVE_STRUCT_TM_TM_ZONE)
        // Use tm_zone fields directly
        tm->tm_gmtoff = tx->tm_gmtoff;
        tm->tm_zone = tx->tm_zone;
        *tzp = -tm->tm_gmtoff;  // Negate for SQL99 compatibility

        if (tzn != NULL) {
            strlcpy(*tzn, tm->tm_zone, MAXTZLEN + 1);
            if (strlen(tm->tm_zone) > MAXTZLEN)
                tm->tm_isdst = -1;  // Signal error
        }
#elif defined(HAVE_INT_TIMEZONE)
        // Use global timezone variables
        *tzp = (tm->tm_isdst > 0) ? TIMEZONE_GLOBAL - SECS_PER_HOUR : TIMEZONE_GLOBAL;

        if (tzn != NULL) {
            strlcpy(*tzn, TZNAME_GLOBAL[tm->tm_isdst], MAXTZLEN + 1);
            if (strlen(TZNAME_GLOBAL[tm->tm_isdst]) > MAXTZLEN)
                tm->tm_isdst = -1;
        }
#else
        // Fallback: default to UTC
        *tzp = 0;
        if (tzn != NULL)
            *tzn = NULL;
#endif
    } else {
        tm->tm_isdst = -1;
    }
}
```