# dt2local

## Location
src/backend/utils/adt/timestamp.c: 2134 - 2146

## Overview
A static helper function that converts a timestamp from UTC to local time by subtracting the specified timezone offset.

## Definition
```c
static Timestamp dt2local(Timestamp dt, int timezone)
```

## Detailed Description
The dt2local function performs timezone conversion by subtracting the timezone offset (in seconds) from the given timestamp. The timezone parameter represents the offset from UTC in seconds, so subtracting it converts from UTC to the local timezone. The conversion is done by multiplying the timezone offset by USECS_PER_SEC to convert from seconds to microseconds, which matches PostgreSQL's internal timestamp representation.

## Parameters / Member Variables
- `dt`: The input timestamp in UTC (Timestamp type)
- `timezone`: The timezone offset from UTC in seconds (positive for timezones ahead of UTC)

## Dependencies
- Functions called/Symbols referenced:
  - Timestamp (timestamp data type)
  - USECS_PER_SEC (constant for microseconds per second)
- Called from (representative examples):
  - [make_timestamptz_at_timezone](../m/make_timestamptz_at_timezone.md)
  - [tm2timestamp](../t/tm2timestamp.md)
  - [timestamp_zone](../t/timestamp_zone.md)
  - [timestamp_izone](../t/timestamp_izone.md)
  - [timestamp2timestamptz_opt_overflow](../t/timestamp2timestamptz_opt_overflow.md)
  - [timestamptz_zone](../t/timestamptz_zone.md)
  - [timestamptz_izone](../t/timestamptz_izone.md)

## Notes and Other Information
This is a static function local to timestamp.c used internally for timezone conversions throughout PostgreSQL's timestamp handling system. The function assumes the input timestamp is in UTC and the timezone parameter follows the conventional sign where positive values represent timezones ahead of UTC (east of Greenwich). The subtraction operation effectively moves the timestamp backward in time to represent the same moment in the local timezone.