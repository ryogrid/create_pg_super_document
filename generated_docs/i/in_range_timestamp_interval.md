# in_range_timestamp_interval

## Location
src/backend/utils/adt/timestamp.c: 3835 - 3875

## Overview
Implements SQL window function RANGE BETWEEN support for timestamp values with interval offsets, determining if a value falls within a specified range from a base timestamp without time zone.

## Definition
```c
Datum in_range_timestamp_interval(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides RANGE BETWEEN clause support for timestamp (without time zone) data types with interval offsets in PostgreSQL window functions. It determines whether a given timestamp (val) falls within a range defined by a base timestamp and an interval offset. The function is similar to in_range_timestamptz_interval but operates on timestamps without time zone information.

The function validates that the offset interval is non-negative per SQL specification requirements and handles special cases involving infinite timestamps and intervals. Unlike its timestamptz counterpart, this function uses DirectFunctionCall2 to invoke timestamp arithmetic functions rather than internal functions.

## Parameters / Member Variables
- `val`: The timestamp value to test for inclusion in the range
- `base`: The base timestamp value from which the range is calculated
- `offset`: The interval offset defining the range size (must be non-negative)
- `sub`: Boolean indicating whether to subtract (true) or add (false) the offset from base
- `less`: Boolean indicating whether to test for less-than-or-equal (true) or greater-than-or-equal (false) comparison

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP
  - PG_GETARG_INTERVAL_P
  - PG_GETARG_BOOL
  - [interval_sign](interval_sign.md)
  - INTERVAL_IS_NOEND
  - TIMESTAMP_IS_NOBEGIN
  - TIMESTAMP_IS_NOEND
  - DatumGetTimestamp
  - DirectFunctionCall2
  - [timestamp_mi_interval](../t/timestamp_mi_interval.md)
  - [timestamp_pl_interval](../t/timestamp_pl_interval.md)
  - TimestampGetDatum
  - [IntervalPGetDatum](../I/IntervalPGetDatum.md)
- Called from:
  - [in_range_date_interval](in_range_date_interval.md) (in src/backend/utils/adt/date.c:1053)

## Notes and Other Information
- Part of SQL window function RANGE BETWEEN support for timestamp without time zone
- Enforces SQL specification requirement that offset intervals must not be negative
- Uses DirectFunctionCall2 mechanism for timestamp arithmetic operations
- Handles infinite timestamp and interval edge cases consistently with other range functions
- Does not currently implement overflow hazard avoidance
- Serves as a foundation for date range functions