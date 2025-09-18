# time_overflows

## Location
src/backend/utils/adt/date.c: 1427 - 1450

## Overview
Validates whether broken-down time-of-day components are within valid ranges and do not exceed 24:00:00 when combined.

## Definition
```c
bool time_overflows(int hour, int min, int sec, fsec_t fsec)
```

## Detailed Description
The time_overflows function performs comprehensive validation of time-of-day components to ensure they represent a valid time within a 24-hour period. It first checks each component individually against their valid ranges, then performs a total time calculation to ensure the combined value does not exceed 24:00:00. The function allows for edge cases like hour=24 or sec=60 (for leap seconds), but ensures the total time remains within a single day.

## Parameters / Member Variables
- `hour`: Hour component (0-24, where 24 represents midnight of the next day)
- `min`: Minute component (0-59)
- `sec`: Second component (0-60, allowing for leap seconds)
- `fsec`: Fractional seconds component in microseconds (0-999999)

## Dependencies
- Functions called/Symbols referenced:
  - HOURS_PER_DAY (constant)
  - MINS_PER_HOUR (constant)
  - SECS_PER_MINUTE (constant)
  - USECS_PER_SEC (constant)
  - USECS_PER_DAY (constant)
- Types used:
  - fsec_t (fractional seconds type)
- Called from (representative examples):
  - [DecodeDateTime](../D/DecodeDateTime.md)
  - [DecodeTimeOnly](../D/DecodeTimeOnly.md)
  - PG_RETURN_TIMETZADT_P

## Notes and Other Information
- Returns true if any component is out of range or the total time exceeds 24:00:00
- Returns false if all components are valid and within range
- Allows edge cases like 24:00:00 (midnight) and leap seconds (60 seconds)
- Essential for input validation in PostgreSQL's date/time parsing functions
- Part of the date/time validation infrastructure in src/backend/utils/adt/date.c