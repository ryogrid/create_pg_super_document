# timestamp2tm

## Location
[src/backend/utils/adt/timestamp.c:1901-1996](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L1901-L1996)

## Overview
Converts a PostgreSQL timestamp data type to a POSIX time structure (struct pg_tm), handling timezone conversion and time component extraction.

## Definition

```c
int
timestamp2tm(Timestamp dt, int *tzp, struct pg_tm *tm, fsec_t *fsec, const char **tzn, pg_tz *attimezone)
```
## Detailed Description
The  function converts a PostgreSQL internal timestamp representation to a human-readable time structure. It performs several key operations:

1. **Time Decomposition**: Splits the timestamp into date and time components using modulo arithmetic
2. **Julian Date Conversion**: Converts the date portion from PostgreSQL's J2000-based epoch to standard Julian dates
3. **Time Component Extraction**: Extracts hours, minutes, seconds, and fractional seconds
4. **Timezone Handling**: Applies timezone conversion when requested, using either the provided timezone or the session default

The function handles edge cases including negative timestamps, out-of-range dates, and timestamps that fall outside the range of . When timezone conversion is not possible, it defaults to GMT.

## Parameters / Member Variables
- `dt`: Input timestamp value to convert
- `*tzp`: Output parameter for timezone offset in seconds (negative of tm_gmtoff), or NULL if no timezone conversion wanted
- `*tm`: Output struct pg_tm to populate with converted time components
- `*fsec`: Output parameter for fractional seconds (microseconds)
- `**tzn`: Output parameter for timezone name string, or NULL if not needed
- `*attimezone`: Timezone to convert to, or NULL to use session_timezone
## Dependencies
- Functions called/Symbols referenced:
  - TMODULO (macro for timestamp/date separation)
  - [j2date](../j/j2date.md) (Julian date to Gregorian date conversion)
  - [dt2time](../d/dt2time.md) (time component extraction)
  - [pg_localtime](../p/pg_localtime.md) (timezone-aware time conversion)
  - USECS_PER_DAY, USECS_PER_SEC, SECS_PER_DAY (time constants)
  - POSTGRES_EPOCH_JDATE, UNIX_EPOCH_JDATE (epoch constants)
- Called from (representative examples):
  - [timestamp_out](timestamp_out.md) (timestamp to string conversion)
  - [timestamp_to_char](timestamp_to_char.md) (formatted timestamp output)
  - [timestamp_pl_interval](timestamp_pl_interval.md) (timestamp arithmetic)
  - [timestamptz_out](timestamptz_out.md) (timestamptz to string conversion)
  - [timestamp_part_common](timestamp_part_common.md) (EXTRACT function implementation)

## Notes and Other Information
- Returns 0 on success, -1 on out of range error
- Year values are full years (not 1900-based like standard C tm structure)
- Month values are 1-based (1-12), not 0-based like standard C
- Handles timestamps outside pg_time_t range by treating them as GMT
- Uses microsecond precision for fractional seconds
- The function is central to PostgreSQL's timestamp handling and is used extensively throughout date/time operations

## Simplified Source

```c
// Simplified version of timestamp2tm
int timestamp2tm(Timestamp dt, int *tzp, struct pg_tm *tm, fsec_t *fsec,
                 const char **tzn, pg_tz *attimezone) {
    Timestamp date;
    Timestamp time;
    pg_time_t utime;

    // Use session timezone if none specified
    if (attimezone == NULL)
        attimezone = session_timezone;

    // Step 1: Split timestamp into date and time components
    time = dt;
    TMODULO(time, date, USECS_PER_DAY);

    // Handle negative time components
    if (time < INT64CONST(0)) {
        time += USECS_PER_DAY;
        date -= 1;
    }

    // Step 2: Convert to Julian date system
    date += POSTGRES_EPOCH_JDATE;

    // Validate date range
    if (date < 0 || date > (Timestamp) INT_MAX)
        return -1;

    // Step 3: Extract date and time components
    j2date((int) date, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
    dt2time(time, &tm->tm_hour, &tm->tm_min, &tm->tm_sec, fsec);

    // Step 4: Handle timezone conversion if requested
    if (tzp == NULL) {
        // No timezone conversion wanted
        tm->tm_isdst = -1;
        tm->tm_gmtoff = 0;
        tm->tm_zone = NULL;
        if (tzn != NULL)
            *tzn = NULL;
        return 0;
    }

    // Convert to Unix epoch for timezone processing
    dt = (dt - *fsec) / USECS_PER_SEC +
         (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY;
    utime = (pg_time_t) dt;

    // Apply timezone conversion if timestamp fits in pg_time_t
    if ((Timestamp) utime == dt) {
        struct pg_tm *tx = pg_localtime(&utime, attimezone);

        // Copy timezone-adjusted values
        tm->tm_year = tx->tm_year + 1900;
        tm->tm_mon = tx->tm_mon + 1;
        tm->tm_mday = tx->tm_mday;
        tm->tm_hour = tx->tm_hour;
        tm->tm_min = tx->tm_min;
        tm->tm_sec = tx->tm_sec;
        tm->tm_isdst = tx->tm_isdst;
        tm->tm_gmtoff = tx->tm_gmtoff;
        tm->tm_zone = tx->tm_zone;
        *tzp = -tm->tm_gmtoff;
        if (tzn != NULL)
            *tzn = tm->tm_zone;
    } else {
        // Out of pg_time_t range: treat as GMT
        *tzp = 0;
        tm->tm_isdst = -1;
        tm->tm_gmtoff = 0;
        tm->tm_zone = NULL;
        if (tzn != NULL)
            *tzn = NULL;
    }

    return 0;
}
```

Key simplifications made:
- Organized into clear sequential steps with descriptive comments
- Simplified variable declarations and flow
- Clarified the dual-phase approach (basic conversion vs timezone conversion)
- Consolidated timezone handling logic
- Preserved all essential date/time arithmetic and edge case handling
- Maintained the distinction between PostgreSQL and standard C time conventions