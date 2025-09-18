# timestamp_zone

## Location
[src/backend/utils/adt/timestamp.c:6164-6228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L6164-L6228)

## Overview
Converts a timestamp to timestamptz by encoding it with a specified time zone, effectively setting the timestamp to represent the specified timezone rather than shifting to it.

## Definition


## Detailed Description
This function implements PostgreSQL's timezone conversion functionality for timestamp values. Unlike typical AT TIME ZONE operations that shift times between zones, timestamp_zone() sets the time to BE in the specified timezone. It takes a timestamp without timezone information and returns a timestamptz by interpreting the input timestamp as being in the specified timezone.

The function handles three types of timezone specifications: fixed-offset abbreviations (like '+05:00'), dynamic-offset abbreviations that depend on the specific date (like 'EST' vs 'EDT'), and full timezone names (like 'America/New_York'). For infinite timestamps, it returns the input unchanged.

The conversion process involves parsing the timezone specification, determining the appropriate offset (which may require historical timezone data lookup), and applying the offset to produce the final timestamptz result.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments containing:
  - zone: Text string specifying the target timezone (abbreviation, offset, or full name)
  - timestamp: The timestamp value to be converted to timestamptz

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring_buffer (timezone string extraction)
  - [DecodeTimezoneName](../D/DecodeTimezoneName.md) (timezone specification parsing)
  - [dt2local](../d/dt2local.md) (timestamp offset application for fixed/dynamic offsets)
  - [timestamp2tm](timestamp2tm.md) (timestamp to broken-down time conversion)
  - [DetermineTimeZoneAbbrevOffset](../D/DetermineTimeZoneAbbrevOffset.md) (dynamic abbreviation offset resolution)
  - DetermineTimeZoneOffset (full timezone offset determination)
  - [tm2timestamp](tm2timestamp.md) (broken-down time to timestamp conversion)
- Constants/Types referenced:
  - TZNAME_FIXED_OFFSET, TZNAME_DYNTZ (timezone type indicators)
  - TZ_STRLEN_MAX (maximum timezone string length)
  - Timestamp, TimestampTz (timestamp data types)
  - [pg_tz](../p/pg_tz.md), pg_tm, fsec_t (timezone and time structures)
- Macros used:
  - TIMESTAMP_NOT_FINITE (infinite timestamp check)
  - IS_VALID_TIMESTAMP (result validation)
  - PG_GETARG_TEXT_PP, PG_GETARG_TIMESTAMP (argument extraction)
  - PG_RETURN_TIMESTAMPTZ (return value)
- Called from:
  - SQL queries using timezone conversion operators and functions

## Notes and Other Information
The function includes comprehensive error handling for out-of-range timestamps and invalid timezone specifications. The distinction between fixed-offset and dynamic-offset timezones is important for historical accuracy - dynamic zones like 'EST' require date-specific offset determination due to daylight saving time transitions. The function preserves infinite timestamp values unchanged, maintaining consistency with PostgreSQL's infinite timestamp semantics.