# feTimestampDifferenceExceeds

## Location
src/bin/pg_basebackup/streamutil.c: 910 - 922

## Overview
A frontend version of PostgreSQL's TimestampDifferenceExceeds() function that checks whether the time difference between two timestamps exceeds a specified number of milliseconds.

## Definition
```c
bool feTimestampDifferenceExceeds(TimestampTz start_time, TimestampTz stop_time, int msec)
```

## Detailed Description
This function provides a frontend implementation for checking if a time interval exceeds a threshold, since client utilities are not linked with backend code. It calculates the difference between two PostgreSQL TimestampTz values and compares it against a millisecond threshold. The function converts the millisecond threshold to microseconds (PostgreSQL's internal precision) by multiplying by 1000 and uses INT64CONST to ensure proper 64-bit arithmetic.

## Parameters / Member Variables
- `start_time`: Starting timestamp (TimestampTz)
- `stop_time`: Ending timestamp (TimestampTz)
- `msec`: Threshold in milliseconds to compare against
- Returns: true if the time difference exceeds the threshold, false otherwise

## Dependencies
- Functions called/Symbols referenced:
  - INT64CONST
- Called from (representative examples):
  - [StreamLogicalLog](../S/StreamLogicalLog.md) (multiple calls in pg_recvlogical.c)
  - [HandleCopyStream](../H/HandleCopyStream.md)

## Notes and Other Information
- Frontend equivalent of backend's TimestampDifferenceExceeds() function
- Uses microsecond precision internally but accepts millisecond threshold for convenience
- Essential for timeout checking and performance monitoring in PostgreSQL client utilities
- The INT64CONST macro ensures proper 64-bit arithmetic regardless of platform
- Returns true even if stop_time is earlier than start_time (negative difference treated as exceeding any positive threshold)