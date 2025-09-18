# timestamp_izone

## Location
src/backend/utils/adt/timestamp.c: 6229 - 6272

## Overview
Converts a timestamp to a timestamptz by applying a specified interval as a time zone offset.

## Definition


## Detailed Description
The timestamp_izone function encodes a timestamp type with a specified time interval as the time zone. It takes an interval representing a time zone offset and a timestamp, then converts the timestamp to a timestamptz by applying the offset. The function performs several validation checks:

1. Handles infinite timestamps by returning them unchanged
2. Validates that the interval is finite 
3. Ensures the interval contains only time components (no months or days)
4. Converts the interval to seconds and applies it as a timezone offset
5. Validates that the resulting timestamp is within valid range

The conversion is performed using the dt2local function which applies the timezone offset to convert from local time to UTC.

## Parameters / Member Variables
-  (Interval*): An interval representing the time zone offset. Must be finite and contain only time components (hours, minutes, seconds), not months or days.
-  (Timestamp): The input timestamp value to be converted to timestamptz.

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INTERVAL_P
  - PG_GETARG_TIMESTAMP  
  - TIMESTAMP_NOT_FINITE
  - INTERVAL_NOT_FINITE
  - dt2local
  - IS_VALID_TIMESTAMP
  - PG_RETURN_TIMESTAMPTZ
  - DirectFunctionCall1
  - DatumGetCString
  - interval_out
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is used internally by PostgreSQL's timestamp handling system
- The function enforces strict validation on the interval parameter to prevent invalid timezone specifications
- Error handling includes specific error codes for different validation failures (ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
- The conversion uses microsecond precision (USECS_PER_SEC) for time calculations
- Located in src/backend/utils/adt/timestamp.c:6229-6272