# DetermineTimeZoneOffset

## Location
src/backend/utils/adt/datetime.c: 1585 - 1606

## Overview
Determines the GMT offset and daylight-savings status for a specific date/time in a given timezone, serving as a public wrapper for the internal timezone offset calculation.

## Definition
```c
int DetermineTimeZoneOffset(struct pg_tm *tm, pg_tz *tzp)
```

## Detailed Description
DetermineTimeZoneOffset is a public interface function that calculates the timezone offset (in seconds from GMT) for a specific date and time within a given timezone. It takes a pg_tm structure with the date/time components already filled in and a timezone definition, then determines both the GMT offset and whether daylight saving time is in effect at that moment.

This function acts as a simple wrapper around DetermineTimeZoneOffsetInternal, providing a cleaner interface for callers who don't need access to the intermediate pg_time_t value. It's essential for converting between local times and UTC in PostgreSQL's timezone-aware operations.

## Parameters / Member Variables
- `tm`: Pointer to pg_tm struct containing date/time fields (tm_year, tm_mon, tm_mday, tm_hour, tm_min, tm_sec) to be evaluated, and will have tm_isdst set on output
- `tzp`: Pointer to timezone definition (zic-style timezone data structure)

## Dependencies
- Functions called/Symbols referenced:
  - DetermineTimeZoneOffsetInternal (the actual implementation)
  - pg_time_t (intermediate time representation type)
  - pg_tm (PostgreSQL's tm structure)
  - pg_tz (timezone definition type)
- Called from (representative examples):
  - DecodeDateTime (during datetime parsing with timezone)
  - timestamptz_pl_interval_internal (timestamp arithmetic)
  - timestamp_zone (timezone conversion operations)
  - date2timestamptz_opt_overflow (date to timestamptz conversion)

## Notes and Other Information
- Returns GMT offset in seconds from UTC (positive for east of GMT, negative for west)
- Sets tm_isdst field: 1 for DST active, 0 for standard time, -1 for unknown
- Returns 0 offset and sets tm_isdst = 0 for dates outside the calculable range
- Does not throw errors for out-of-range dates, leaving error handling to higher-level code
- Critical for timezone-aware timestamp operations in PostgreSQL