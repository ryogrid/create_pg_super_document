# feTimestampDifference

## Location
[src/bin/pg_basebackup/streamutil.c:888-909](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/streamutil.c#L888-L909)

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
  - [StreamLogicalLog](../S/StreamLogicalLog.md)
  - [CalculateCopyStreamSleeptime](../C/CalculateCopyStreamSleeptime.md)

## Notes and Other Information
- Frontend equivalent of backend's TimestampDifference() function
- Returns zero values for both seconds and microseconds when stop_time <= start_time
- Uses PostgreSQL's internal timestamp format (microseconds since PostgreSQL epoch)
- Essential for timing and performance monitoring in PostgreSQL client utilities
- The microseconds component is always in the range 0-999999

## Simplified Source

```c
void feTimestampDifference(TimestampTz start_time, TimestampTz stop_time,
                          long *secs, int *microsecs) {
    // Calculate the difference in microseconds
    TimestampTz diff = stop_time - start_time;

    // Handle negative or zero differences
    if (diff <= 0) {
        *secs = 0;
        *microsecs = 0;
    } else {
        // Convert to seconds and remaining microseconds
        *secs = (long) (diff / USECS_PER_SEC);
        *microsecs = (int) (diff % USECS_PER_SEC);
    }
}
```