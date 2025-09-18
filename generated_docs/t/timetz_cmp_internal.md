# timetz_cmp_internal

## Location
src/backend/utils/adt/date.c: 2443 - 2469

## Overview
A static internal comparison function that compares two time with timezone values, performing a two-level comparison: first by GMT-equivalent time, then by timezone offset if times are equal.

## Definition
```c
static int timetz_cmp_internal(TimeTzADT *time1, TimeTzADT *time2)
```

## Detailed Description
This function implements the core comparison logic for PostgreSQL's time with timezone data type. It performs a hierarchical comparison:

1. **Primary comparison**: Converts both time values to GMT-equivalent time by adding the timezone offset in microseconds, then compares these normalized times
2. **Secondary comparison**: If the GMT-equivalent times are equal, it compares the timezone offsets directly

This approach ensures that two timetz values are considered equal only if both their time component and timezone component are identical, even if they represent the same instant in different timezones.

The function returns:
- 1 if time1 > time2
- -1 if time1 < time2  
- 0 if time1 == time2

## Parameters / Member Variables
- `time1`: Pointer to the first TimeTzADT structure to compare
- `time2`: Pointer to the second TimeTzADT structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - TimeTzADT (data type)
  - TimeOffset (data type)
  - USECS_PER_SEC (constant for microseconds per second conversion)
- Called from (representative examples):
  - [timetz_eq](timetz_eq.md)
  - [timetz_ne](timetz_ne.md)
  - [timetz_lt](timetz_lt.md)
  - [timetz_le](timetz_le.md)
  - [timetz_gt](timetz_gt.md)
  - [timetz_ge](timetz_ge.md)
  - [timetz_cmp](timetz_cmp.md)
  - [timetz_larger](timetz_larger.md)
  - [timetz_smaller](timetz_smaller.md)
  - [in_range_timetz_interval](../i/in_range_timetz_interval.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same compilation unit (date.c)
- The two-level comparison ensures proper ordering semantics for time with timezone values
- The conversion to GMT-equivalent time allows comparison of times across different timezones
- Used as the foundation for all timetz comparison operators and functions