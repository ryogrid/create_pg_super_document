# float_time_overflows

## Location
[src/backend/utils/adt/date.c:1451-1487](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1451-L1487)

## Overview
Validates time-of-day components where seconds are represented as a floating-point value, handling special cases like NaN and performing precise range checking.

## Definition
```c
bool float_time_overflows(int hour, int min, double sec)
```

## Detailed Description
The float_time_overflows function provides validation for time components where the seconds field is a double-precision floating-point number containing both integral and fractional seconds. It performs similar validation to time_overflows but includes special handling for floating-point edge cases such as NaN values and precision issues. The function rounds the seconds value to microsecond precision before validation to avoid unexpected errors due to imprecise floating-point representation.

## Parameters / Member Variables
- `hour`: Hour component (0-24, where 24 represents midnight of the next day)
- `min`: Minute component (0-59)
- `sec`: Seconds component as a double value, including fractional seconds

## Dependencies
- Functions called/Symbols referenced:
  - HOURS_PER_DAY (constant)
  - MINS_PER_HOUR (constant)
  - SECS_PER_MINUTE (constant)
  - USECS_PER_SEC (constant)
  - USECS_PER_DAY (constant)
  - isnan (math function)
  - rint (math function)
- Types used:
  - int64 (for casting seconds to integer)
- Called from (representative examples):
  - [make_time](../m/make_time.md)
  - [make_timestamp_internal](../m/make_timestamp_internal.md)
  - PG_RETURN_TIMETZADT_P

## Notes and Other Information
- Returns true if any component is out of range, if sec is NaN, or if total time exceeds 24:00:00
- Returns false if all components are valid and within range
- Uses rint() to round seconds to microsecond precision before validation
- Handles floating-point edge cases more carefully than the integer version
- Essential for validating floating-point time inputs in PostgreSQL's time construction functions
- Part of the date/time validation infrastructure in src/backend/utils/adt/date.c

## Simplified Source

```c
bool float_time_overflows(int hour, int min, double sec) {
    // Check hour and minute ranges
    if (hour < 0 || hour > HOURS_PER_DAY || min < 0 || min >= MINS_PER_HOUR)
        return true;

    // Handle floating-point seconds: check for NaN and round to microseconds
    if (isnan(sec))
        return true;
    sec = rint(sec * USECS_PER_SEC);
    if (sec < 0 || sec > SECS_PER_MINUTE * USECS_PER_SEC)
        return true;

    // Check total time doesn't exceed 24:00:00
    if (((((hour * MINS_PER_HOUR + min) * SECS_PER_MINUTE) * USECS_PER_SEC) + (int64) sec) > USECS_PER_DAY)
        return true;

    return false;
}
```