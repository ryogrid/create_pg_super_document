# DetermineTimeZoneAbbrevOffsetTS

## Location
src/backend/utils/adt/datetime.c: 1784 - 1820

## Overview
Determines the GMT offset and DST flag for a dynamic time zone abbreviation using a TimestampTz (UTC time) as the probe time, returning DST status via output parameter rather than modifying input structure.

## Definition
```c
int DetermineTimeZoneAbbrevOffsetTS(TimestampTz ts, const char *abbr, pg_tz *tzp, int *isdst)
```

## Detailed Description
This function is a variant of `DetermineTimeZoneAbbrevOffset()` that takes a TimestampTz (UTC timestamp) as the probe time instead of a broken-down time structure. The function determines the appropriate GMT offset for a timezone abbreviation at the specified UTC time.

The function follows a two-step approach:
1. First attempts to match the abbreviation directly against timezone data using `DetermineTimeZoneAbbrevOffsetInternal()`
2. If no direct match is found, converts the timestamp to local time components using `timestamp2tm()` and falls back to standard timezone offset determination with `DetermineTimeZoneOffset()`

The DST status is returned through the `isdst` output parameter rather than being stored in a tm structure field.

## Parameters / Member Variables
- `ts`: TimestampTz value representing the UTC time at which to determine the timezone offset
- `abbr`: The timezone abbreviation string to match against timezone data
- `tzp`: Pointer to the timezone definition structure
- `isdst`: Output parameter that receives the DST flag (0 for standard time, 1 for daylight time)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_tz](../p/pg_tz.md) (struct)
  - [timestamptz_to_time_t](../t/timestamptz_to_time_t.md)
  - pg_time_t (type)
  - [pg_tm](../p/pg_tm.md) (struct)
  - fsec_t (type)
  - [DetermineTimeZoneAbbrevOffsetInternal](DetermineTimeZoneAbbrevOffsetInternal.md)
  - [timestamp2tm](../t/timestamp2tm.md)
  - DetermineTimeZoneOffset
- Called from (representative examples):
  - [timetz_zone](../t/timetz_zone.md)
  - [pg_timezone_abbrevs](../p/pg_timezone_abbrevs.md)
  - [timestamptz_zone](../t/timestamptz_zone.md)

## Notes and Other Information
- This is a convenience wrapper that allows timezone abbreviation offset determination using UTC timestamps directly
- Handles timestamp out of range errors by reporting appropriate error messages
- More efficient than the basic version when working with TimestampTz values since it avoids unnecessary conversions
- The function will report an error if the input timestamp is out of the valid range for conversion
- Located in src/backend/utils/adt/datetime.c:1784-1820