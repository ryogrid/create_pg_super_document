# timestamptz_izone

## Location
src/backend/utils/adt/timestamp.c: 6466 - 6505

## Overview
This function converts a timestamp with time zone to a timestamp without time zone by applying a specified time interval as a timezone offset, effectively treating the interval as a fixed timezone displacement.

## Definition


## Detailed Description
The  function performs timezone conversion using an interval value to specify the timezone offset. Unlike  which accepts timezone names or abbreviations, this function accepts an interval that represents the timezone offset from UTC.

The function validates that the interval contains only time components (hours, minutes, seconds, microseconds) and rejects intervals that include months or days, since these cannot be reliably converted to a fixed timezone offset. The interval's time component is converted to seconds and used as a timezone offset.

The conversion process:
1. Validates that the timestamp is finite
2. Ensures the interval is finite and contains no month/day components
3. Extracts the time portion of the interval and converts it to seconds
4. Applies the offset using  to convert the timestamptz to local time
5. Validates the resulting timestamp is within valid range

## Parameters / Member Variables
- Argument 0:  (Interval*) - The timezone offset specified as an interval value
- Argument 1:  (TimestampTz) - The input timestamp with timezone to convert

## Dependencies
- Functions called/Symbols referenced:
  -  - retrieves the interval argument
  -  - retrieves the timestamptz argument
  -  - checks for infinite timestamp values
  -  - checks for infinite interval values
  -  - calls PostgreSQL functions for error formatting
  -  - converts interval to string representation for error messages
  -  - extracts C string from Datum
  -  - converts pointer to Datum
  -  - applies timezone offset to convert timestamptz to local time
  -  - validates the resulting timestamp
  -  - returns the converted timestamp result
  -  - constant for microseconds per second conversion
- Called from:
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- This function implements timezone conversion using interval notation (e.g., INTERVAL '+05:00:00')
- The function explicitly rejects intervals with month or day components to ensure predictable timezone offset behavior
- Located in  at lines 6466-6505
- The timezone offset is calculated as negative of the interval's time component (zone->time / USECS_PER_SEC)
- Comprehensive error handling includes validation of both input parameters and the resulting timestamp
- The function follows PostgreSQL's V1 calling convention for SQL functions
- Error messages include the string representation of invalid intervals for better user feedback
- Used typically with the AT TIME ZONE syntax when the timezone is specified as an interval rather than a name
- The conversion treats the interval as a fixed offset, making it suitable for simple timezone arithmetic