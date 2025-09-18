# timestamptz_trunc_internal

## Location
src/backend/utils/adt/timestamp.c: 4826 - 4969

## Overview
Internal implementation function that provides timezone-aware timestamp truncation functionality, shared by both `timestamptz_trunc` and `timestamptz_trunc_zone`.

## Definition
```c
static TimestampTz timestamptz_trunc_internal(text *units, TimestampTz timestamp, pg_tz *tzp)
```

## Detailed Description
The `timestamptz_trunc_internal` function is the core implementation for timezone-aware timestamp truncation operations. Unlike the plain `timestamp_trunc` function, this version properly handles timezone conversions during the truncation process.

Key differences from plain timestamp truncation:
1. Accepts a timezone parameter (`pg_tz *tzp`) for timezone-aware operations
2. Uses `timestamp2tm` with timezone information to break down the timestamp
3. Sets `redotz = true` for truncations at day level and above, indicating timezone offset recalculation is needed
4. Calls `DetermineTimeZoneOffset` when `redotz` is true to handle potential DST transitions
5. Uses `tm2timestamp` with timezone information to reconstruct the final timestamp

This function is essential for ensuring that timestamp truncation behaves correctly across different timezones and handles edge cases like daylight saving time transitions appropriately.

## Parameters / Member Variables
- `units` (text*): The time unit to truncate to (e.g., 'year', 'month', 'day', 'hour', etc.)
- `timestamp` (TimestampTz): The timezone-aware timestamp to be truncated  
- `tzp` (pg_tz*): The timezone context for the truncation operation

## Dependencies
- Functions called/Symbols referenced:
  - [downcase_truncate_identifier](../d/downcase_truncate_identifier.md)
  - [DecodeUnits](../D/DecodeUnits.md)
  - [timestamp2tm](timestamp2tm.md)
  - [date2isoweek](../d/date2isoweek.md)
  - [isoweek2date](../i/isoweek2date.md)
  - DetermineTimeZoneOffset
  - [tm2timestamp](tm2timestamp.md)
  - [format_type_be](../f/format_type_be.md)
  - Various DTK_* constants (DTK_WEEK, DTK_YEAR, DTK_MONTH, etc.)
  - MONTHS_PER_YEAR
- Called from (representative examples):
  - [timestamptz_trunc](timestamptz_trunc.md)
  - [timestamptz_trunc_zone](timestamptz_trunc_zone.md)

## Notes and Other Information
- This is an internal static function that consolidates the timezone-aware truncation logic
- The `redotz` flag determines when timezone offset recalculation is necessary (for truncations at day level and above)
- Properly handles timezone transitions that may occur when truncating to day boundaries
- Uses the same time unit constants and validation logic as the non-timezone version
- Includes special handling for weeks using ISO week calculations
- Millennium/century/decade calculations use the same boundary logic as plain timestamp truncation
- The function assumes that infinite timestamps have already been handled by the caller
- Critical for maintaining timezone correctness during truncation operations