# timestamp_izone

## Location
[src/backend/utils/adt/timestamp.c:6229-6272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L6229-L6272)

## Overview
Converts a timestamp to a timestamptz by applying a specified interval as a time zone offset.

## Definition

```c
Datum
timestamp_izone(PG_FUNCTION_ARGS)
```
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
  - [dt2local](../d/dt2local.md)
  - IS_VALID_TIMESTAMP
  - PG_RETURN_TIMESTAMPTZ
  - DirectFunctionCall1
  - [DatumGetCString](../D/DatumGetCString.md)
  - [interval_out](../i/interval_out.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is used internally by PostgreSQL's timestamp handling system
- The function enforces strict validation on the interval parameter to prevent invalid timezone specifications
- Error handling includes specific error codes for different validation failures (ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
- The conversion uses microsecond precision (USECS_PER_SEC) for time calculations
- Located in src/backend/utils/adt/timestamp.c:6229-6272

## Simplified Source

```c
Datum timestamp_izone(PG_FUNCTION_ARGS) {
    Interval *zone = PG_GETARG_INTERVAL_P(0);
    Timestamp timestamp = PG_GETARG_TIMESTAMP(1);
    TimestampTz result;
    int tz;

    // Handle infinite timestamps
    if (TIMESTAMP_NOT_FINITE(timestamp))
        PG_RETURN_TIMESTAMPTZ(timestamp);

    // Validate interval zone is finite
    if (INTERVAL_NOT_FINITE(zone))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("interval time zone \"%s\" must be finite",
                       DatumGetCString(DirectFunctionCall1(interval_out,
                                                           PointerGetDatum(zone))))));

    // Validate interval contains only time components
    if (zone->month != 0 || zone->day != 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("interval time zone \"%s\" must not include months or days",
                       DatumGetCString(DirectFunctionCall1(interval_out,
                                                           PointerGetDatum(zone))))));

    // Convert interval to seconds offset
    tz = zone->time / USECS_PER_SEC;

    // Apply timezone offset
    result = dt2local(timestamp, tz);

    // Validate result is in range
    if (!IS_VALID_TIMESTAMP(result))
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                errmsg("timestamp out of range")));

    PG_RETURN_TIMESTAMPTZ(result);
}
```