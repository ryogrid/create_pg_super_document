# timestamp2tm

## Location
src/backend/utils/adt/timestamp.c: 1901 - 1996

## Overview
Converts a PostgreSQL timestamp data type to a POSIX time structure (struct pg_tm), handling timezone conversion and time component extraction.

## Definition


## Detailed Description
The  function converts a PostgreSQL internal timestamp representation to a human-readable time structure. It performs several key operations:

1. **Time Decomposition**: Splits the timestamp into date and time components using modulo arithmetic
2. **Julian Date Conversion**: Converts the date portion from PostgreSQL's J2000-based epoch to standard Julian dates
3. **Time Component Extraction**: Extracts hours, minutes, seconds, and fractional seconds
4. **Timezone Handling**: Applies timezone conversion when requested, using either the provided timezone or the session default

The function handles edge cases including negative timestamps, out-of-range dates, and timestamps that fall outside the range of . When timezone conversion is not possible, it defaults to GMT.

## Parameters / Member Variables
- : Input timestamp value to convert
- : Output parameter for timezone offset in seconds (negative of tm_gmtoff), or NULL if no timezone conversion wanted
- : Output struct pg_tm to populate with converted time components
- : Output parameter for fractional seconds (microseconds)
- : Output parameter for timezone name string, or NULL if not needed
- : Timezone to convert to, or NULL to use session_timezone

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