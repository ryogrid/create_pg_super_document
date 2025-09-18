# timetz_izone

## Location
src/backend/utils/adt/date.c: 3122 - 3164

## Overview
Converts a time with time zone to a different time zone specified by an interval offset, providing precise timezone adjustment using PostgreSQL interval types.

## Definition


## Detailed Description
`timetz_izone` is a PostgreSQL built-in function that converts a time with time zone (TIMETZ) value using an interval as the timezone specification. Unlike `timetz_zone` which accepts text-based timezone names, this function takes a PostgreSQL interval value representing the timezone offset. The interval must contain only a time component (hours, minutes, seconds) and cannot include months or days. The function performs the same timezone conversion logic as `timetz_zone` but provides a more programmatic interface for precise timezone offset calculations.

The conversion adjusts the time component by the difference between the original timezone and the interval-specified timezone, ensuring the result remains within a 24-hour period through modular arithmetic.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `zone` (Interval*): The interval specifying the target timezone offset (must be finite and contain only time components)
  - `time` (TimeTzADT*): The input time with time zone value to convert

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INTERVAL_P
  - PG_GETARG_TIMETZADT_P
  - INTERVAL_NOT_FINITE
  - DirectFunctionCall1
  - interval_out
  - DatumGetCString
  - PointerGetDatum
  - palloc
  - PG_RETURN_TIMETZADT_P
  - ereport
- Called from (representative examples):
  - SQL AT TIME ZONE expressions with interval specifications
  - Programmatic timezone conversion operations

## Notes and Other Information
- Validates that the interval is finite and contains no month or day components, only time-based offsets
- Converts interval time to timezone offset using: `tz = -(zone->time / USECS_PER_SEC)`
- Sign inversion (-) converts interval representation to timezone offset convention
- Same time adjustment algorithm as `timetz_zone`: `result->time = time->time + (time->zone - tz) * USECS_PER_SEC`
- Includes modular arithmetic to handle day boundary crossings correctly
- Error reporting provides detailed interval representation in error messages for debugging
- More suitable for programmatic use cases where timezone offsets are calculated rather than named
- Provides exact control over timezone offset without DST rule interpretation