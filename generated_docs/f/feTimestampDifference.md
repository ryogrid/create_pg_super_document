# feTimestampDifference

## Location
src/bin/pg_basebackup/streamutil.c: 888 - 909

## Overview
A frontend version of PostgreSQL's TimestampDifference() function that calculates the time difference between two timestamps and returns the result in seconds and microseconds.

## Definition
```c
void feTimestampDifference(TimestampTz start_time, TimestampTz stop_time, long *secs, int *microsecs)
```

## Detailed Description
This function provides a frontend implementation of timestamp difference calculation since client utilities are not linked with backend code. It computes the difference between two PostgreSQL TimestampTz values and breaks down the result into separate seconds and microseconds components. The function handles the case where stop_time is earlier than or equal to start_time by returning zero values, ensuring non-negative results.

## Parameters / Member Variables
- `start_time`: Starting timestamp (TimestampTz)
- `stop_time`: Ending timestamp (TimestampTz)
- `secs`: Pointer to store the seconds component of the difference
- `microsecs`: Pointer to store the microseconds component of the difference

## Dependencies
- Functions called/Symbols referenced:
  - USECS_PER_SEC
- Called from (representative examples):
  - StreamLogicalLog
  - CalculateCopyStreamSleeptime

## Notes and Other Information
- Frontend equivalent of backend's TimestampDifference() function
- Returns zero values for both seconds and microseconds when stop_time <= start_time
- Uses PostgreSQL's internal timestamp format (microseconds since PostgreSQL epoch)
- Essential for timing and performance monitoring in PostgreSQL client utilities
- The microseconds component is always in the range 0-999999